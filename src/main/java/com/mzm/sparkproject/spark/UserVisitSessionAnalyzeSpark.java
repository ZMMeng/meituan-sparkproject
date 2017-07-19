package com.mzm.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.ITaskDao;
import com.mzm.sparkproject.dao.impl.DaoFactory;
import com.mzm.sparkproject.domain.Task;
import com.mzm.sparkproject.test.MockData;
import com.mzm.sparkproject.utils.ParamUtils;
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
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD) {

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

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        return null;
    }
}
