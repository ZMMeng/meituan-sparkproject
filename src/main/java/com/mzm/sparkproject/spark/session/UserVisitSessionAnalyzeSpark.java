package com.mzm.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.*;
import com.mzm.sparkproject.dao.factory.DaoFactory;
import com.mzm.sparkproject.domain.*;
import com.mzm.sparkproject.test.MockData;
import com.mzm.sparkproject.utils.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析和spark作业
 * Created by Administrator on 2017/7/19.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        args = new String[]{"1"};
        //构建Spark Context
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION) //设置Spark作业名称
                .setMaster("local") //本地测试
                .set("spark.storage.memoryFraction", "0.5") //设置cache操作内存占比为50%
                .set("spark.shuffle.consolidateFiles", "true") //设置合并map端输出文件
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //启用Kryo序列化机制
                .registerKryoClasses(new Class[]{CategorySecondarySortKey.class,
                        IntList.class, IntArrayList.class}); //注册需要使用Kryo序列化机制的自定义类
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

        //将行为数据RDD转换为(会话ID,行为数据)的RDD
        JavaPairRDD<String, Row> session2ActionRDD = getSessionId2ActionRDD(actionRDD);
        //将这一RDD持久化，仅内存级别
        session2ActionRDD = session2ActionRDD.persist(StorageLevel.MEMORY_ONLY());

        //对行为数据按session粒度进行聚合
        //(Row,) -> (sessionId,Row) -> (sessionId,Iterable<Row>) -> (userId,partAggrInfo)
        //(Row,) -> (userId,userInfo)
        //(userId,partAggrInfo) + (userId,userInfo) -> (userId,(userInfo,partAggrInfo)) -> (sessionId,
        // fullAggrInfo)
        JavaPairRDD<String, String> sessionId2FullAggeInfoRDD = aggregateBySession(session2ActionRDD, sqlContext);

        /*System.out.println(sessionId2FullAggeInfoRDD.count());

        for (Tuple2<String, String> tuple : sessionId2FullAggeInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }*/

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        //按照使用者指定的筛选参数进行数据过滤
        JavaPairRDD<String, String> filteredSessionId2FullAggrInfoRDD =
                filterSessionAndAggrStat(sessionId2FullAggeInfoRDD, taskParam, sessionAggrStatAccumulator);
        //将这一RDD持久化，仅内存级别
        filteredSessionId2FullAggrInfoRDD = filteredSessionId2FullAggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        //获取通过筛选条件的session的访问明细数据RDD，作为公共的RDD
        JavaPairRDD<String, Row> filteredSessionId2DetailRDD =
                getFilteredSession2AggrInfoRDD(filteredSessionId2FullAggrInfoRDD, session2ActionRDD);
        //将这一RDD持久化，仅内存级别
        filteredSessionId2DetailRDD = filteredSessionId2DetailRDD.persist(StorageLevel.MEMORY_ONLY());

        //System.out.println(filteredSessionId2FullAggrInfoRDD.count());
        randomExtractSession(taskId, filteredSessionId2FullAggrInfoRDD, session2ActionRDD, sc);

        /*for (Tuple2<String, String> tuple : filteredSessionId2FullAggrInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }*/

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());

        //获取top10热门品类，并将相应的统计信息插入MySQL中
        List<Tuple2<CategorySecondarySortKey, String>> top10CategoryList = getTop10Category(filteredSessionId2DetailRDD,
                taskId);

        //获取Top10活跃session
        getTop10Sessions(taskId, filteredSessionId2DetailRDD, top10CategoryList, sc);

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
     * @param sc         Spark Context对象
     * @param sqlContext SQL Context
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

        String sql = "select * from user_visit_action where date>='" + startDate + "' and date<'" + endDate
                + "'";
        DataFrame actionDf = sqlContext.sql(sql);

        return actionDf.javaRDD();
    }

    /**
     * 获取sessionId到访问行为数据的映射的RDD
     * (Row) -> (sessionId, Row)
     *
     * @param actionRDD 行为数据
     * @return sessionId到访问行为数据的映射的RDD
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String sessionId = row.getString(2);
                return new Tuple2<>(sessionId, row);
            }
        });
    }

    /**
     * 对行为数据按session粒度进行聚合 ((sessionId,Row) -> (sessionId, Row))
     * 行为数据：(sessionId,Row) -> (sessionId,Iterable<Row>) -> (userId,partAggrInfo)
     * 用户信息：(Row,) -> (userId,userInfo)
     * (userId,partAggrInfo) + (userId,userInfo) -> (userId,(userInfo,partAggrInfo)) -> (sessionId,
     * fullAggrInfo)
     * 同时计算每一个session的时长和步长，优化性能
     *
     * @param session2ActionRDD (会话ID,行为数据)的RDD
     * @return session粒度聚合后的行为数据RDD
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaPairRDD<String, Row> session2ActionRDD, SQLContext sqlContext) {

        /*//将行为数据映射成元组对(会话ID,行为数据)
        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(
                *//**
                 * 第一个参数是输入
                 * 第二个和第三个参数是输出，对应key和value
                 * Row -> (String, Row)
                 *//*
                new PairFunction<Row, String, Row>() {
                    *//**
                     * 将一行行为数据映射成元组对(会话ID,行为数据)
                     *
                     * @param row 一行行为数据
                     * @return 元组对(sessionId, row)
                     * @throws Exception
                     *//*
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(2), row);
                    }
                });*/
        //对行为数据按照sessionId进行分组
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = session2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuilder searchKeywordSB = new StringBuilder();
                        StringBuilder clickCategoryIdSB = new StringBuilder();

                        Long userId = null;

                        //session的起始时间和结束时间
                        Date startTime = null;
                        Date endTime = null;

                        //session的访问步长
                        int stepLength = 0;

                        //遍历会话的所有行为
                        while (iterator.hasNext()) {
                            //获取搜索词和点击品类ID
                            Row row = iterator.next();
                            String searchKeyword = row.getString(5);
                            String clickCategoryId = row.getString(6);
                            if (userId == null) {
                                userId = Long.valueOf(row.getString(1));
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

                            //获取操作时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            //给startTime和endTime赋初始值
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            //比较actionTime、startTime和endTime的大小
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            //每一个动作(一条记录)，就是一步
                            stepLength++;
                        }

                        //获取组合后的搜索关键词和点击品类ID，同时去除最后的","
                        String searchKeywords = StringUtils.trimComma(searchKeywordSB.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdSB.toString());

                        //计算会话时长，以秒为单位
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        //将会话ID、组合后的搜索关键词、组合后的点击品类ID、会话时长和访问步长组合成部分聚合信息
                        StringBuilder sb = new StringBuilder();
                        sb.append(Constants.FIELD_SESSION_ID).append("=").append(sessionId).append("|");
                        if (StringUtils.isNotEmpty(searchKeywords)) {
                            sb.append(Constants.FIELD_SEARCH_KEYWORDS).append("=").append(searchKeywords)
                                    .append("|");
                        }
                        if (StringUtils.isNotEmpty(clickCategoryIds)) {
                            sb.append(Constants.FIELD_CLICK_CATEGORY_IDS).append("=").append
                                    (clickCategoryIds).append
                                    ("|");
                        }
                        sb.append(Constants.FIELD_VISIT_LENGTH).append("=").append(visitLength).append("|")
                                .append(Constants.FIELD_STEP_LENGTH).append("=").append(stepLength).append
                                ("|")
                                .append(Constants.FIELD_START_TIME).append("=").append(DateUtils.formatTime
                                (startTime)).append("|");

                        String _partAggrInfo = sb.toString();
                        if (_partAggrInfo.endsWith("|")) {
                            _partAggrInfo = _partAggrInfo.substring(0, _partAggrInfo.length() - 1);
                        }
                        final String partAggrInfo = _partAggrInfo;

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

        //拼接join后的数据，(userId,(userInfo,partAggrInfo)) -> (sessionId,fullAggrInfo)
        JavaPairRDD<String, String> sessionId2FullAggeInfoRDD = userId2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        //获取部分聚合信息
                        String partAggrInfo = tuple._2._1;
                        //获取用户信息
                        Row userInfoRow = tuple._2._2;
                        //从部分聚合信息中获取会话ID
                        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|",
                                Constants.FIELD_SESSION_ID);
                        //从用户信息中获取年龄、职业、城市和性别
                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String gender = userInfoRow.getString(6);

                        //将部分聚合信息、年龄、职业、城市和性别组成完整的聚合信息
                        String fullAggrInfo = new StringBuilder(partAggrInfo).append("|")
                                .append(Constants.FIELD_AGE).append("=").append(age).append("|")
                                .append(Constants.FIELD_PROFESSIONAL).append("=").append(professional)
                                .append("|")
                                .append(Constants.FIELD_CITY).append("=").append(city).append("|")
                                .append(Constants.FIELD_GENDER).append("=").append(gender)
                                .toString();

                        return new Tuple2<>(sessionId, fullAggrInfo);
                    }
                });
        return sessionId2FullAggeInfoRDD;
    }

    /**
     * 按照使用者指定的筛选参数进行数据过滤，同时进行会话的访问时长和访问步长的统计
     *
     * @param sessionid2AggrInfoRDD      按session粒度进行聚合后的用户行为数据
     * @param taskParam                  使用者指定的筛选参数
     * @param sessionAggrStatAccumulator 自定义的Accumulator
     * @return 过滤以及聚合后的用户行为数据
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                                                        final JSONObject taskParam, Accumulator<String>
                                                                                sessionAggrStatAccumulator) {

        //从任务参数中获取筛选参数
        //获取年龄范围的起始年龄和终止年龄
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        //获取职业范围
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        //获取城市范围
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        //获取性别范围
        String gender = ParamUtils.getParam(taskParam, Constants.PARAM_GENDER);
        //获取搜索关键词
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        //获取品类ID
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        //重新组装筛选参数字符串
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotEmpty(startAge)) {
            sb.append(Constants.PARAM_START_AGE).append("=").append(startAge).append("|");
        }
        if (StringUtils.isNotEmpty(endAge)) {
            sb.append(Constants.PARAM_END_AGE).append("=").append(endAge).append("|");
        }
        if (StringUtils.isNotEmpty(professionals)) {
            sb.append(Constants.PARAM_PROFESSIONALS).append("=").append(professionals).append("|");
        }
        if (StringUtils.isNotEmpty(cities)) {
            sb.append(Constants.PARAM_CITIES).append("=").append(cities).append("|");
        }
        if (StringUtils.isNotEmpty(gender)) {
            sb.append(Constants.PARAM_GENDER).append("=").append(gender).append("|");
        }
        if (StringUtils.isNotEmpty(keywords)) {
            sb.append(Constants.PARAM_KEYWORDS).append("=").append(keywords).append("|");
        }
        if (StringUtils.isNotEmpty(categoryIds)) {
            sb.append(Constants.PARAM_START_AGE).append("=").append(categoryIds);
        }
        String _parameter = sb.toString();
        if (StringUtils.isNotEmpty(_parameter) && _parameter.endsWith("|")) {
            //去除最后的"|"
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;

        //按照筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //获取聚合数据
                        String aggrInfo = tuple._2;

                        //按照筛选条件进行过滤
                        //按照年龄范围进行过滤 (startAge, endAge)
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants
                                        .PARAM_START_AGE,
                                Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        //按照职业范围过滤 professionals
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }
                        //按照城市范围过滤 cities
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants
                                .PARAM_CITIES)) {
                            return false;
                        }
                        //按照性别进行过滤 gender
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_GENDER, parameter, Constants
                                .PARAM_GENDER)) {
                            return false;
                        }
                        //按照搜索关键词进行过滤 keywords
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants
                                .PARAM_KEYWORDS)) {
                            return false;
                        }
                        //按照品类ID进行过滤 categoryIds
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
                                Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        //开始统计会话的访问时长和访问步长
                        //会话计数自增1
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                        //获取访问时长和访问步长
                        Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
                                Constants.FIELD_VISIT_LENGTH));
                        Integer stepLength = Integer.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,
                                "\\|",
                                Constants.FIELD_STEP_LENGTH));
                        //统计会话的访问时长和步长
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);
                        return true;
                    }

                    /**
                     * 统计会话的访问时长范围
                     *
                     * @param visitLength 会话访问时长
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        }
                        if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        }
                        if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        }
                        if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        }
                        if (visitLength >= 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        }
                        if (visitLength >= 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        }
                        if (visitLength >= 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        }
                        if (visitLength >= 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        }
                        if (visitLength >= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 统计会话的访问步长范围
                     *
                     * @param stepLength 会话步长
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        }
                        if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        }
                        if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        }
                        if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        }
                        if (stepLength >= 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        }
                        if (stepLength >= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                });
        return filteredSessionId2AggrInfoRDD;
    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     *
     * @param filteredSession2AggrInfoRDD 通过筛选条件的session的聚合统计信息
     * @param sessionId2ActionRDD (会话ID,行为数据)的RDD
     * @return 通过筛选条件的session的访问明细数据RDD
     */
    private static JavaPairRDD<String, Row> getFilteredSession2AggrInfoRDD(JavaPairRDD<String, String> filteredSession2AggrInfoRDD,
                                                                   JavaPairRDD<String, Row> sessionId2ActionRDD){

        JavaPairRDD<String, Row> filteredSessionId2DetailRDD = filteredSession2AggrInfoRDD
                .join(sessionId2ActionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws
                            Exception {
                        return new Tuple2<>(tuple._1, tuple._2._2);
                    }
                });
        return filteredSessionId2DetailRDD;
    }

    /**
     * 随机抽取session
     *
     * @param taskId                任务ID
     * @param sessionid2AggrInfoRDD (会话ID,聚合信息数据)的RDD
     * @param sessionid2ActionRDD   (会话ID,行为数据)的RDD
     * @param sc                    Sparck Context
     */
    private static void randomExtractSession(final long taskId, JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                             JavaPairRDD<String, Row> sessionid2ActionRDD, JavaSparkContext sc) {

        //计算每天每小时的会话数量
        //(sessionId, fullAggrInfo) -> (dateHour, fullAggrInfo)
        //countByKey
        JavaPairRDD<String, String> time2FullInfoRDD = sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String fullAggrInfo = tuple._2;

                        String startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|",
                                Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);
                        return new Tuple2<>(dateHour, fullAggrInfo);
                    }
                });
        //获取每天每小时的会话数量
        Map<String, Object> countMap = time2FullInfoRDD.countByKey();

        //按时间比例随机抽取session，计算出该session的索引
        //将每天每小时的会话数量，转变为每天的24个小时的会话数量
        //即{yyyy-MM-dd_HH : count} -> {yyyy-MM-dd : {HH : count}}
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : countMap.entrySet()) {
            String dateHour = entry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(entry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);

            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }

        //开始计算抽取session的索引
        //按天数平分抽取session总数
        int extractNumberPerDay = Constants.EXTRACT_NUMBER / dateHourCountMap.size();

        //存储抽取session索引的Map，按照天、小时分类
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        Random random = new Random();

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            //获取日期以及对应的24个小时的session数量
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            //计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            //开始遍历这一天的每个小时的情况
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                //获取当前小时以及对应的会话总数
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                //这一小时抽取session数量 = 这一小时的session数量 / 这一天的session数量 * 每天抽取的session数量
                int hourExtractNumber = (int) (1.0 * count / sessionCount * extractNumberPerDay);

                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //生成这一小时需要抽取的session的索引
                int extractIndex;
                for (int i = 0; i < hourExtractNumber; i++) {
                    extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        //使用fastutil中的IntList
        Map<String, Map<String, IntList>> fastUtilDateHourExtractMap = new HashMap<>();

        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();

            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
            Map<String, IntList> fastUtilHourExtractMap = new HashMap<>();

            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();

                List<Integer> extractList = hourExtractEntry.getValue();
                IntList fastUtilExtractList = new IntArrayList();

                fastUtilExtractList.addAll(extractList);
                fastUtilHourExtractMap.put(hour, fastUtilExtractList);
            }

            fastUtilDateHourExtractMap.put(date, fastUtilHourExtractMap);
        }

        //将dateHourExtractMap设置成广播变量
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
                sc.broadcast(fastUtilDateHourExtractMap);

        //遍历每天每小时的session，然后根据随机索引进行抽取
        //(dateHour, fullAggrInfo) -> (dateHour, Iterable<fullAggrInfo>)
        JavaPairRDD<String, Iterable<String>> time2FullInfosRDD = time2FullInfoRDD.groupByKey();

        //使用flatMap遍历所有的(dateHour, Iterable<fullAggrInfo>)，并抽取指定索引的session
        //抽取指定索引的session，直接写入到MySQL中，同时记录并返回sessionId
        //(dateHour, Iterable<fullAggrInfo>) -> (sessionId, sessionId)
        JavaPairRDD<String, String> extractSessionIdsRDD = time2FullInfosRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple)
                            throws
                            Exception {
                        //用于存储抽取会话ID的List
                        List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();

                        //获取日期和小时的信息
                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];

                        //获取广播变量中封装的dateHourExtractMap
                        Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.value();

                        //获取这一天这一小时抽取的session索引
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        //获取抽取session模块的DAO层实现类对象
                        ISessionRandomExtractDao sessionRandomExtractDao = DaoFactory
                                .getSessionRandomExtractDaoImpl();

                        //开始迭代
                        //获取迭代器
                        Iterator<String> iterator = tuple._2.iterator();
                        //记录全部session的索引
                        int index = 0;
                        while (iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();

                            if (extractIndexList.contains(index)) {
                                //遍历到session的索引在抽取索引范围内
                                //获取会话ID
                                String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo,
                                        "\\|",
                                        Constants.FIELD_SESSION_ID);
                                //将该会话的信息写入MySQL中
                                //将抽取会话的信息封装
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskId(taskId);
                                sessionRandomExtract.setSessionId(sessionId);
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,
                                        "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo,
                                        "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo,
                                        "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                                sessionRandomExtractDao.insert(sessionRandomExtract);
                                //将该会话ID加入到List中
                                extractSessionIds.add(new Tuple2<>(sessionId, sessionId));
                            }
                            //索引自增
                            index++;
                        }
                        return extractSessionIds;
                    }
                });

        //获取抽取session的明细数据
        //将抽取的会话ID与行为数据join起来
        //(sessionId,sessionId) + (sessionId, Row) -> (sessionId,(sessionId,Row))
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionid2ActionRDD);

        //遍历每一个抽取的会话，将其详细信息插入MySQL中
        extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                //获取抽取会话的全部信息
                Row row = tuple._2._2;

                //封装抽取会话的详细信息
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(Long.valueOf(row.getString(1)));
                sessionDetail.setSessionId(tuple._1);
                sessionDetail.setPageId(Long.valueOf(row.getString(3)));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(Long.valueOf(row.getString(6) == null ? "0" : row
                        .getString(6)));
                sessionDetail.setClickProductId(Long.valueOf(row.getString(7) == null ? "0" : row.getString
                        (7)));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                //将抽取会话的详细信息插入MySQL中
                ISessionDetailDao sessionDetailDao = DaoFactory.getSessionDetailDaoImpl();
                sessionDetailDao.insert(sessionDetail);
            }
        });
    }

    /**
     * 计算各个会话范围占比，并写入MySQL
     *
     * @param value  Accumulator的value?
     * @param taskId 任务ID
     */
    private static void calculateAndPersistAggrStat(String value, long taskId) {
        //System.out.println(value);
        long sessionCount = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.SESSION_COUNT));
        long timePeriod1sTo3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_1s_3s));
        long timePeriod4sTo6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_4s_6s));
        long timePeriod7sTo9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_7s_9s));
        long timePeriod10sTo30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_10s_30s));
        long timePeriod30sTo60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_30s_60s));
        long timePeriod1mTo3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_1m_3m));
        long timePeriod3mTo10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_3m_10m));
        long timePeriod10mTo30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_10m_30m));
        long timePeriod30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.TIME_PERIOD_30m));

        long stepLength1To3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.STEP_PERIOD_1_3));
        long stepLength4To6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.STEP_PERIOD_4_6));
        long stepLength7To9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.STEP_PERIOD_7_9));
        long stepLength10To30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.STEP_PERIOD_10_30));
        long stepLength30To60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.STEP_PERIOD_30_60));
        long stepLength60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constants.STEP_PERIOD_60));

        double timePeriod1sTo3sRatio = NumberUtils.formatDouble(1.0 * timePeriod1sTo3s / sessionCount, 2);
        double timePeriod4sTo6sRatio = NumberUtils.formatDouble(1.0 * timePeriod4sTo6s / sessionCount, 2);
        double timePeriod7sTo9sRatio = NumberUtils.formatDouble(1.0 * timePeriod7sTo9s / sessionCount, 2);
        double timePeriod10sTo30sRatio = NumberUtils.formatDouble(1.0 * timePeriod10sTo30s / sessionCount, 2);
        double timePeriod30sTo60sRatio = NumberUtils.formatDouble(1.0 * timePeriod30sTo60s / sessionCount, 2);
        double timePeriod1mTo3mRatio = NumberUtils.formatDouble(1.0 * timePeriod1mTo3m / sessionCount, 2);
        double timePeriod3mTo10mRatio = NumberUtils.formatDouble(1.0 * timePeriod3mTo10m / sessionCount, 2);
        double timePeriod10mTo30mRatio = NumberUtils.formatDouble(1.0 * timePeriod10mTo30m / sessionCount, 2);
        double timePeriod30mRatio = NumberUtils.formatDouble(1.0 * timePeriod30m / sessionCount, 2);

        double stepLength1To3Ratio = NumberUtils.formatDouble(1.0 * stepLength1To3 / sessionCount, 2);
        double stepLength4To6Ratio = NumberUtils.formatDouble(1.0 * stepLength4To6 / sessionCount, 2);
        double stepLength7To9Ratio = NumberUtils.formatDouble(1.0 * stepLength7To9 / sessionCount, 2);
        double stepLength10To30Ratio = NumberUtils.formatDouble(1.0 * stepLength10To30 / sessionCount, 2);
        double stepLength30To60Ratio = NumberUtils.formatDouble(1.0 * stepLength30To60 / sessionCount, 2);
        double stepLength60Ratio = NumberUtils.formatDouble(1.0 * stepLength60 / sessionCount, 2);

        //将统计结果封装
        SessionAggrStat sessionAggrStat = new SessionAggrStat();

        sessionAggrStat.setTaskId(taskId);
        sessionAggrStat.setSessionCount(sessionCount);
        sessionAggrStat.setTimePeriod1sTo3sRatio(timePeriod1sTo3sRatio);
        sessionAggrStat.setTimePeriod4sTo6sRatio(timePeriod4sTo6sRatio);
        sessionAggrStat.setTimePeriod7sTo9sRatio(timePeriod7sTo9sRatio);
        sessionAggrStat.setTimePeriod10sTo30sRatio(timePeriod10sTo30sRatio);
        sessionAggrStat.setTimePeriod30sTo60sRatio(timePeriod30sTo60sRatio);
        sessionAggrStat.setTimePeriod1mTo3mRatio(timePeriod1mTo3mRatio);
        sessionAggrStat.setTimePeriod3mTo10mRatio(timePeriod3mTo10mRatio);
        sessionAggrStat.setTimePeriod10mTo30mRatio(timePeriod10mTo30mRatio);
        sessionAggrStat.setTimePeriod30mRatio(timePeriod30mRatio);
        sessionAggrStat.setStepLength1To3Ratio(stepLength1To3Ratio);
        sessionAggrStat.setStepLength4To6Ratio(stepLength4To6Ratio);
        sessionAggrStat.setStepLength7To9Ratio(stepLength7To9Ratio);
        sessionAggrStat.setStepLength10To30Ratio(stepLength10To30Ratio);
        sessionAggrStat.setStepLength30To60Ratio(stepLength30To60Ratio);
        sessionAggrStat.setStepLength60Ratio(stepLength60Ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDao sessionAggrStatDAO = DaoFactory.getSessionAggrStatDaoImpl();
        sessionAggrStatDAO.insert(sessionAggrStat);

    }

    /**
     * 获取top10热门品类，并将相应的统计信息插入MySQL中
     *
     * @param filteredSessionId2DetailRDD 通过筛选条件的session的访问明细数据RDD
     * @param taskId                      任务ID
     * @return top10热门品类的统计结果
     */
    private static List<Tuple2<CategorySecondarySortKey, String>> getTop10Category(
            JavaPairRDD<String, Row> filteredSessionId2DetailRDD, long taskId) {


        //获取所有session访问过的品类ID(点击、下单和支付)
        JavaPairRDD<Long, Long> categoryIdRDD = filteredSessionId2DetailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

                        Row row = tuple._2;

                        List<Tuple2<Long, Long>> list = new ArrayList<>();
                        //点击的品类ID
                        String categoryId = row.getString(6);
                        if (StringUtils.isNotEmpty(categoryId)) {
                            list.add(new Tuple2<>(Long.valueOf(categoryId), Long.valueOf(categoryId)));
                        }
                        //下单的品类ID
                        String orderCategoryIds = row.getString(8);
                        if (StringUtils.isNotEmpty(orderCategoryIds)) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                            }
                        }
                        //支付的品类ID
                        String payCategoryIds = row.getString(10);
                        if (StringUtils.isNotEmpty(payCategoryIds)) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                            }
                        }
                        return list;
                    }
                });

        //对得到所有session访问过的品类ID(点击、下单和支付)去重
        categoryIdRDD = categoryIdRDD.distinct();

        //分别过滤过各品类的点击、下单和支付行为，再计算各品类的点击、下单和支付计数
        //各品类的点击计数RDD
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(filteredSessionId2DetailRDD);
        //各品类的下单计数RDD
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(filteredSessionId2DetailRDD);
        //各品类的支付计数RDD
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(filteredSessionId2DetailRDD);

        //所有session访问过的品类ID分别与各品类的点击计数、下单计数和支付计数进行左外连接
        JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoryAndData(categoryIdRDD, clickCategoryId2CountRDD,
                orderCategoryId2CountRDD, payCategoryId2CountRDD);

        //自定义二次排序Key
        //将所有session访问过的品类的点击计数、下单计数和支付计数的RDD进行二次排序
        //(categoryId,categoryId + clickCount + orderCount + payCount) -> (sortKey,categoryId + clickCount + orderCount + payCount)
        JavaPairRDD<CategorySecondarySortKey, String> sortKey2CountRDD = categoryId2CountRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySecondarySortKey, String>() {

                    @Override
                    public Tuple2<CategorySecondarySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {

                        String countInfo = tuple._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                                Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                                Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                                Constants.FIELD_PAY_COUNT));

                        CategorySecondarySortKey sortKey = new CategorySecondarySortKey(clickCount, orderCount,
                                payCount);
                        return new Tuple2<>(sortKey, countInfo);
                    }
                });

        //进行二次排序
        JavaPairRDD<CategorySecondarySortKey, String> sortedCategory2CountRDD = sortKey2CountRDD.sortByKey(false);

        //取出Top10热门品类
        List<Tuple2<CategorySecondarySortKey, String>> top10CategoryList = sortedCategory2CountRDD.take(10);

        //将Top10热门品类的统计结果插入到MySQL中
        ITop10CategoryDao top10CategoryDao = DaoFactory.getTop10CategoryDaoImpl();
        for(Tuple2<CategorySecondarySortKey, String> tuple : top10CategoryList){
            String countInfo = tuple._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                    Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                    Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                    Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|",
                    Constants.FIELD_PAY_COUNT));

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskId(taskId);
            top10Category.setCategoryId(categoryId);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            top10CategoryDao.insert(top10Category);
        }

        return top10CategoryList;

    }

    /**
     * 获取各品类点击次数的RDD
     *
     * @param sessionId2ActionRDD (会话ID,行为数据)的RDD
     * @return 各品类点击次数的RDD
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2ActionRDD) {

        JavaPairRDD<String, Row> clickActionRDD = sessionId2ActionRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return StringUtils.isNotEmpty(row.getString(6));
                    }
                })
                //.coalesce(100) 对过滤后的数据进行分区压缩
                ;
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                        long clickCategoryId = Long.valueOf(tuple._2.getString(6));
                        return new Tuple2<>(clickCategoryId, 1L);
                    }
                });
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return clickCategoryId2CountRDD;
    }

    /**
     * 获取各品类下单次数的RDD
     *
     * @param sessionId2ActionRDD (会话ID,行为数据)的RDD
     * @return 各品类下单次数的RDD
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row>
                                                                               sessionId2ActionRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionId2ActionRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return StringUtils.isNotEmpty(row.getString(8));
                    }
                });
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list;
                    }
                });
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return orderCategoryId2CountRDD;
    }

    /**
     * 获取各品类支付次数的RDD
     *
     * @param sessionId2ActionRDD (会话ID,行为数据)的RDD
     * @return 各品类支付次数的RDD
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row>
                                                                             sessionId2ActionRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionId2ActionRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return StringUtils.isNotEmpty(row.getString(10));
                    }
                });
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(10);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list;
                    }
                });
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return payCategoryId2CountRDD;
    }

    /**
     * 将所有session访问过的品类ID分别与各品类的点击计数、下单计数和支付计数进行左外连接
     * (categoryId,categoryId) + (categoryId,clickCount) + (categoryId,orderCount) + (categoryId,payCount) ->
     * (categoryId,categoryId + clickCount + orderCount + payCount)
     *
     * @param categoryIdRDD            所有session访问过的品类ID的RDD
     * @param clickCategoryId2CountRDD 各品类的点击计数RDD
     * @param orderCategoryId2CountRDD 各品类的下单计数RDD
     * @param payCategoryId2CountRDD   各品类支付计数RDD
     * @return 所有session访问过的品类的点击计数、下单计数和支付计数的RDD
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryIdRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {

        //考虑每一个品类ID不一定都有点击、下单和支付，所以使用左外连接

        return categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {

                        long categoryId = tuple._1;
                        Optional<Long> clickCountOptional = tuple._2._2;

                        long clickCount = clickCountOptional.isPresent() ? clickCountOptional.get() : 0L;

                        StringBuilder sb = new StringBuilder();
                        String value = sb.append(Constants.FIELD_CATEGORY_ID).append("=").append(categoryId).append("|")
                                .append(Constants.FIELD_CLICK_COUNT).append("=").append(clickCount).append("|")
                                .toString();
                        return new Tuple2<>(categoryId, value);
                    }
                })
                .leftOuterJoin(orderCategoryId2CountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {

                        long categoryId = tuple._1;
                        Optional<Long> orderCountOptional = tuple._2._2;

                        long orderCount = orderCountOptional.isPresent() ? orderCountOptional.get() : 0L;

                        StringBuilder sb = new StringBuilder(tuple._2._1);
                        String value = sb.append(Constants.FIELD_ORDER_COUNT).append("=").append(orderCount).append("|")
                                .toString();
                        return new Tuple2<>(categoryId, value);
                    }
                })
                .leftOuterJoin(payCategoryId2CountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {

                        long categoryId = tuple._1;
                        Optional<Long> payCountOptional = tuple._2._2;

                        long payCount = payCountOptional.isPresent() ? payCountOptional.get() : 0L;

                        StringBuilder sb = new StringBuilder(tuple._2._1);
                        String value = sb.append(Constants.FIELD_PAY_COUNT).append("=").append(payCount).toString();
                        return new Tuple2<>(categoryId, value);
                    }
                });
    }

    /**
     * 获取Top10活跃session，并将统计结果写入MySQL中
     *
     * @param taskId                      任务ID
     * @param filteredSessionId2DetailRDD 通过筛选条件的session的访问明细数据RDD
     * @param top10CategoryList           top10品类统计分析结果
     * @param sc                          Spark Context
     */
    private static void getTop10Sessions(final long taskId, JavaPairRDD<String, Row> filteredSessionId2DetailRDD,
                                         List<Tuple2<CategorySecondarySortKey, String>> top10CategoryList,
                                         JavaSparkContext sc){

        //将top10热门品类的ID生成RDD
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySecondarySortKey, String> category : top10CategoryList) {
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(category._2, "\\|",
                    Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryId, categoryId));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);

        //计算top10热门品类被各个session点击的次数
        JavaPairRDD<String, Iterable<Row>> filteredSessionId2DetailsRDD = filteredSessionId2DetailRDD.groupByKey();
        JavaPairRDD<Long, String> categoryId2SessionCountRDD = filteredSessionId2DetailsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    @Override
                    public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        //获取sessionId
                        String sessionId = tuple._1;
                        //获取迭代器
                        Iterator<Row> iterator = tuple._2.iterator();

                        //记录这一session对每个品类的点击次数的Map
                        Map<Long, Long> categoryCountMap = new HashMap<>();

                        //计算出这一session对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (StringUtils.isNotEmpty(row.getString(6))) {
                                long categoryId = Long.valueOf(row.getString(6));

                                Long count = categoryCountMap.get(categoryId);

                                if (count == null) {
                                    count = 0L;
                                }

                                count++;

                                categoryCountMap.put(categoryId, count);
                            }
                        }

                        List<Tuple2<Long, String>> list = new ArrayList<>();

                        for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()){
                            long categoryId = categoryCountEntry.getKey();
                            long count = categoryCountEntry.getValue();

                            String value = new StringBuilder().append(sessionId).append(",").append(count).toString();

                            list.add(new Tuple2<>(categoryId, value));
                        }
                        return list;
                    }
                });

        //获取top10热门品类被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
                .join(categoryId2SessionCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {

                        return new Tuple2<>(tuple._1, tuple._2._2);
                    }
                });

        //分组取TopN算法实现，获取每个品类的top10活跃用户
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                        //获取品类ID
                        long categoryId = tuple._1;
                        //获取该品类被各个session点击次数的迭代器
                        Iterator<String> iterator = tuple._2.iterator();

                        //记录点击这一品类次数前十位的session，但是此处可以考虑使用二叉平衡树为底层的数据结构来进行插入更好
                        String[] top10Sessions = new String[10];

                        while (iterator.hasNext()) {
                            //获取这一session对这一品类的点击情况，包括sessionId和点击次数
                            String sessionCount = iterator.next();
                            String sessionId = sessionCount.split(",")[0];
                            long count = Long.valueOf(sessionCount.split(",")[1]);

                            //获取top10Sessions
                            for (int i = 0; i < top10Sessions.length; i++) {
                                if (top10Sessions[i] == null) {
                                    //此时top10Sessions[i]为空，且点击次数count小于这一索引前面的点击次数
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    //top10Sessions已满，需进行比较
                                    //top10Sessions此时这一位置存储的点击次数
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    //比较两者点击次数大小
                                    if (count > _count) {
                                        //大于，意味着要进行插入，将数组从这一位开始，依次向后移动一位，数组的最后一位舍弃
                                        for (int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }

                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }

                                }
                            }
                        }

                        //将结果写入MySQL中
                        List<Tuple2<String, String>> list = new ArrayList<>();

                        ITop10SessionDao top10SessionDao = DaoFactory.getTop10SessionDaoImpl();

                        for (String sessionCount : top10Sessions) {
                            if(sessionCount != null){
                                String sessionId = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);

                                //封装
                                Top10Session top10Session = new Top10Session();
                                top10Session.setTaskId(taskId);
                                top10Session.setCategoryId(categoryId);
                                top10Session.setSessionId(sessionId);
                                top10Session.setClickCount(count);

                                top10SessionDao.insert(top10Session);

                                list.add(new Tuple2<>(sessionId, sessionId));
                            }
                        }
                        return list;
                    }
                });

        //获取top10活跃session的详细信息，并插入MySQL中
        JavaPairRDD<String, Tuple2<String, Row>> top10SessionDetailRDD =
                top10SessionRDD.join(filteredSessionId2DetailRDD);
        top10SessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {

                //获取抽取会话的全部信息
                Row row = tuple._2._2;

                //封装抽取会话的详细信息
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(Long.valueOf(row.getString(1)));
                sessionDetail.setSessionId(tuple._1);
                sessionDetail.setPageId(Long.valueOf(row.getString(3)));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(Long.valueOf(row.getString(6) == null ? "0" : row.getString(6)));
                sessionDetail.setClickProductId(Long.valueOf(row.getString(7) == null ? "0" : row.getString(7)));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                //将抽取会话的详细信息插入MySQL中
                ISessionDetailDao sessionDetailDao = DaoFactory.getSessionDetailDaoImpl();
                sessionDetailDao.insert(sessionDetail);
            }
        });
    }

}
