package com.mzm.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.ITaskDao;
import com.mzm.sparkproject.dao.impl.DaoFactory;
import com.mzm.sparkproject.domain.Task;
import com.mzm.sparkproject.test.MockData;
import com.mzm.sparkproject.utils.ParamUtils;
import com.mzm.sparkproject.utils.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析和spark作业
 * Created by Administrator on 2017/7/19.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        //构建Spark Context
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION) //设置Spark作业名称
                .setMaster("local"); //本地测试
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockDate(sc, sqlContext);

        //创建需要使用的DAO组件
        ITaskDao taskDao = DaoFactory.getTaskDaoImpl();
        //获取指定的任务id
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        //获取指定的任务
        Task task = taskDao.findById(taskId);
        //将任务参数封装成JSON格式数据
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //从user_visit_action表中，查询出指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        JavaPairRDD<String, String> sessionId2FullAggeInfoRDD = aggregateBySession(actionRDD, sqlContext);

        //将Spark Context关闭
        sc.close();
    }

    /**
     * 获取SQL Context
     *
     * @param sc Spark Context对象
     * @return SQL Context
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        }
        return new HiveContext(sc);
    }

    /**
     * 生成模拟数据(本地模式生效)
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockDate(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQL Context
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {

        //从JSON数据中获取起始日期和结束日期
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where date>=" + startDate + " and date<" + endDate +
                ";";
        DataFrame actionDf = sqlContext.sql(sql);

        return actionDf.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合 (Row -> (sessionId, Row))
     *
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合后的行为数据RDD
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext
            sqlContext) {

        //将行为数据映射成元组对(会话ID,行为数据)
        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(
                /**
                 * 第一个参数是输入
                 * 第二个和第三个参数是输出，对应key和value
                 * Row -> (String, Row)
                 */
                new PairFunction<Row, String, Row>() {
                    /**
                     * 将一行行为数据映射成元组对(会话ID,行为数据)
                     *
                     * @param row 一行行为数据
                     * @return 元组对(sessionId, row)
                     * @throws Exception
                     */
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(2), row);
                    }
                });
        //对行为数据按照sessionId进行分组
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = session2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws
                            Exception {
                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuilder searchKeywordSB = new StringBuilder();
                        StringBuilder clickCategoryIdSB = new StringBuilder();

                        Long userId = null;
                        //遍历会话的所有行为
                        while (iterator.hasNext()) {
                            //获取搜索词和点击品类ID
                            Row row = iterator.next();
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);
                            if (userId == null) {
                                userId = row.getLong(1);
                            }

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                //必须非空
                                if (!searchKeywordSB.toString().contains(searchKeyword)) {
                                    //不能重复
                                    searchKeywordSB.append(searchKeyword).append(",");
                                }
                            }

                            if (clickCategoryId != null) {
                                //必须非空
                                if (!clickCategoryIdSB.toString().contains(String.valueOf(clickCategoryId))) {
                                    //不能重复
                                    clickCategoryIdSB.append(clickCategoryId).append(",");
                                }
                            }
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordSB.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdSB.toString());

                        String partAggrInfo = new StringBuilder().
                                append(Constants.FIELD_SESSION_ID).append("=").append(sessionId).append("|").
                                append(Constants.FIELD_SEARCH_KEYWORDS).append("=").append(searchKeywords)
                                .append("|").
                                        append(Constants.FIELD_CLICK_CATEGORY_IDS).append("=").append
                                        (clickCategoryIds)
                                .toString();

                        return new Tuple2<>(userId, partAggrInfo);
                    }
                });
        //查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        //将用户数据映射成<userId, Row>
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getLong(0), row);
                    }
                });

        //将session粒度数据与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join
                (userId2InfoRDD);

        //拼接join后的数据，(userId,(userInfo,partAggrInfo)) ---> (sessionId,fullAggrInfo)
        JavaPairRDD<String, String> sessionId2FullAggeInfoRDD = userId2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        //获取部分聚合信息
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|",
                                Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String gender = userInfoRow.getString(6);

                        String fullAggrInfo = new StringBuilder(partAggrInfo).append("|").
                                append(Constants.FIELD_AGE).append("=").append(age).append("|").
                                append(Constants.FIELD_PROFESSIONAL).append("=").append(professional).append("|").
                                append(Constants.FIELD_CITY).append("=").append(city).append("|").
                                append(Constants.FIELD_GENDER).append("=").append(gender).toString();
                        return new Tuple2<>(sessionId, fullAggrInfo);
                    }
                });
        return sessionId2FullAggeInfoRDD;
    }
}
