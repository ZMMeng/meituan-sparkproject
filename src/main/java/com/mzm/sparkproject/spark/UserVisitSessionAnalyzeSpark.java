package com.mzm.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.ISessionAggrStatDao;
import com.mzm.sparkproject.dao.ITaskDao;
import com.mzm.sparkproject.dao.impl.DaoFactory;
import com.mzm.sparkproject.domain.SessionAggrStat;
import com.mzm.sparkproject.domain.Task;
import com.mzm.sparkproject.test.MockData;
import com.mzm.sparkproject.utils.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

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

        //对行为数据按session粒度进行聚合
        //(Row,) -> (sessionId,Row) -> (sessionId,Iterable<Row>) -> (userId,partAggrInfo)
        //(Row,) -> (userId,userInfo)
        //(userId,partAggrInfo) + (userId,userInfo) -> (userId,(userInfo,partAggrInfo)) -> (sessionId,fullAggrInfo)
        JavaPairRDD<String, String> sessionId2FullAggeInfoRDD = aggregateBySession(actionRDD, sqlContext);

        /*System.out.println(sessionId2FullAggeInfoRDD.count());

        for (Tuple2<String, String> tuple : sessionId2FullAggeInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }*/

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        //按照使用者指定的筛选参数进行数据过滤
        JavaPairRDD<String, String> filteredSessionId2FullAggrInfoRDD = filterSessionAndAggrStat
                (sessionId2FullAggeInfoRDD, taskParam, sessionAggrStatAccumulator);

        System.out.println(filteredSessionId2FullAggrInfoRDD.count());

        /*for (Tuple2<String, String> tuple : filteredSessionId2FullAggrInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }*/

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
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

        String sql = "select * from user_visit_action where date>='" + startDate + "' and date<'" + endDate + "'";
        DataFrame actionDf = sqlContext.sql(sql);

        return actionDf.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合 (Row -> (sessionId, Row))
     * 行为数据：(Row,) -> (sessionId,Row) -> (sessionId,Iterable<Row>) -> (userId,partAggrInfo)
     * 用户信息：(Row,) -> (userId,userInfo)
     * (userId,partAggrInfo) + (userId,userInfo) -> (userId,(userInfo,partAggrInfo)) -> (sessionId,fullAggrInfo)
     * 同时计算每一个session的时长和步长，优化性能
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
                            sb.append(Constants.FIELD_SEARCH_KEYWORDS).append("=").append(searchKeywords).append("|");
                        }
                        if (StringUtils.isNotEmpty(clickCategoryIds)) {
                            sb.append(Constants.FIELD_CLICK_CATEGORY_IDS).append("=").append(clickCategoryIds).append
                                    ("|");
                        }
                        sb.append(Constants.FIELD_VISIT_LENGTH).append("=").append(visitLength).append("|")
                                .append(Constants.FIELD_STEP_LENGTH).append("=").append(stepLength).append("|");

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
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

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
                                .append(Constants.FIELD_PROFESSIONAL).append("=").append(professional).append("|")
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
     * @param sessionid2AggrInfoRDD 按session粒度进行聚合后的用户行为数据
     * @param taskParam             使用者指定的筛选参数
     * @return
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
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE,
                                Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        //按照职业范围过滤 professionals
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }
                        //按照城市范围过滤 cities
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }
                        //按照性别进行过滤 gender
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_GENDER, parameter, Constants.PARAM_GENDER)) {
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
                        Integer stepLength = Integer.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
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
                    private void calculateVisitLength(long visitLength){
                        if(visitLength >= 1 && visitLength <= 3){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        }
                        if(visitLength >= 4 && visitLength <= 6){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        }
                        if(visitLength >= 7 && visitLength <= 9){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        }
                        if(visitLength >= 10 && visitLength <= 30){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        }
                        if(visitLength >= 30 && visitLength <= 60){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        }
                        if(visitLength >= 60 && visitLength <= 180){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        }
                        if(visitLength >= 180 && visitLength <= 600){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        }
                        if(visitLength >= 600 && visitLength <= 1800){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        }
                        if(visitLength >= 1800){
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 统计会话的访问步长范围
                     *
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength){
                        if(stepLength >= 1 && stepLength <= 3){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        }
                        if(stepLength >= 4 && stepLength <= 6){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        }
                        if(stepLength >= 7 && stepLength <= 9){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        }
                        if(stepLength >= 10 && stepLength <= 30){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        }
                        if(stepLength >= 30 && stepLength <= 60){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        }
                        if(stepLength >= 60){
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                });
        return filteredSessionId2AggrInfoRDD;
    }

    /**
     * 计算各个会话范围占比，并写入MySQL
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskId){
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
}
