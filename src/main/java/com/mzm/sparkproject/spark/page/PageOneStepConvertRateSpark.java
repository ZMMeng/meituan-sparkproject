package com.mzm.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.IPageSplitConvertRateDao;
import com.mzm.sparkproject.dao.ITaskDao;
import com.mzm.sparkproject.dao.factory.DaoFactory;
import com.mzm.sparkproject.domain.PageSplitConvertRate;
import com.mzm.sparkproject.domain.Task;
import com.mzm.sparkproject.utils.DateUtils;
import com.mzm.sparkproject.utils.NumberUtils;
import com.mzm.sparkproject.utils.ParamUtils;
import com.mzm.sparkproject.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率模块Spark作业
 * Created by Administrator on 2017/7/24.
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {
        //构造上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //生成模拟数据
        SparkUtils.mockDate(sc, sqlContext);

        //查询任务，获取任务参数
        //创建需要使用的DAO组件
        ITaskDao taskDao = DaoFactory.getTaskDaoImpl();
        //获取指定的任务id
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        //获取指定的任务
        Task task = taskDao.findById(taskId);
        if (task == null) {
            System.out.println(new Date() + "找不到相应的任务，任务ID为" + taskId);
            return;
        }
        //将任务参数封装成JSON格式数据
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //根据任务参数，查询指定时间范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        //将行为数据RDD转换为(会话ID,行为数据)的RDD
        JavaPairRDD<String, Row> session2ActionRDD = getSessionId2ActionRDD(actionRDD);
        //持久化
        session2ActionRDD.persist(StorageLevel.MEMORY_ONLY());

        //将行为数据按照sessionId进行划分，(sessionId, Row) -> (sessionId, Iterable<Row>)
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();

        //每个session单跳页面切片的生成，以及页面流的匹配算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(session2ActionsRDD, taskParam, sc);

        //获取每个切片的PV
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        //获取起始页面的PV
        long startPagePv = getStartPagePv(taskParam, session2ActionsRDD);

        //计算各个切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);

        //向MySQL中插入页面切片转化率统计结果
        persistConvertRate(convertRateMap, taskId);

        //关闭上下文
        sc.close();
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
     * 页面切片生成与匹配算法
     *
     * @param sessionId2ActionsRDD 按session分类的用户访问行为数据
     * @param taskParam            任务参数
     * @param sc                   Spark Context
     * @return 页面切片
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD, JSONObject taskParam, JavaSparkContext sc) {

        //获取页面流任务参数
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);

        //获取使用者指定的页面流中各个页面组成的数组
        String[] targetPages = targetPageFlow.split(",");

        //生成用户指定的页面流切片
        List<String> targetPageSplits = new ArrayList<>();
        for (int i = 1; i < targetPages.length; i++){
            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
            targetPageSplits.add(targetPageSplit);
        }

        //将用户指定的页面流切片设置成广播变量
        final Broadcast<List<String>> targetPageSplitsBroadcast = sc.broadcast(targetPageSplits);

        return sessionId2ActionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        //返回值list
                        List<Tuple2<String, Integer>> list = new ArrayList<>();

                        //该sessionId对应的访问行为的迭代器
                        Iterator<Row> iterator = tuple._2.iterator();

                        //获取用户指定的页面流切片
                        List<String> targetPageSplits = targetPageSplitsBroadcast.value();

                        //对该session的访问行为按照操作时间进行排序(默认没有排序)
                        List<Row> rows = new ArrayList<>();
                        while (iterator.hasNext()) {
                            rows.add(iterator.next());
                        }

                        //使用自定义的Comparator，对行为数据按操作时间进行排序
                        Collections.sort(rows, new Comparator<Row>() {
                            @Override
                            public int compare(Row o1, Row o2) {
                                //从两者的行为数据中获取操作时间
                                String actionTime1 = o1.getString(4);
                                String actionTime2 = o2.getString(4);

                                Date date1 = DateUtils.parseTime(actionTime1);
                                Date date2 = DateUtils.parseTime(actionTime2);

                                return date1.compareTo(date2);
                            }
                        });

                        //生成页面切片
                        Long lastPageId = null;
                        for (Row row : rows) {
                            long pageId = Long.valueOf(row.getString(3));

                            if(lastPageId == null){
                                lastPageId = pageId;
                                continue;
                            }

                            //生成一个页面切片
                            String pageSplit = new StringBuilder().append(lastPageId).append("_").append(pageId)
                                    .toString();

                            //判断生成的切片是否在用户给定的页面流中
                            if(targetPageSplits.contains(pageSplit)){
                                list.add(new Tuple2<>(pageSplit, 1));
                            }

                            lastPageId = pageId;
                        }
                        return list;
                    }
                });
    }

    /**
     * 获取页面流中初始页面的PV
     *
     * @param taskParam            任务参数
     * @param sessionId2ActionsRDD 按session分类的用户访问行为数据
     * @return 页面流中初始页面的PV
     */
    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD){

        //获取页面流任务参数
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);

        //初始页面ID
        final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = sessionId2ActionsRDD.flatMap(
                new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {
                    @Override
                    public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        //返回值list
                        List<Long> list = new ArrayList<>();

                        //该sessionId对应的访问行为的迭代器
                        Iterator<Row> iterator = tuple._2.iterator();

                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            long pageId = Long.valueOf(row.getString(3));
                            if(pageId == startPageId){
                                list.add(pageId);
                            }
                        }
                        return list;
                    }
                });

        return startPageRDD.count();
    }

    /**
     * 计算页面切片转化率
     *
     * @param taskParam      任务参数
     * @param pageSplitPvMap 页面切片PV
     * @param startPagePv    起始页面PV
     * @return 页面切片转化率
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap, long startPagePv){

        Map<String, Double> convertRateMap = new HashMap<>();

        //获取使用者指定的页面流中各个页面组成的数组
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        long lastPageSplitPv = 0L;

        List<String> targetPageSplits = new ArrayList<>();
        for (int i = 1; i < targetPages.length; i++) {
            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
            long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

            double convertRate = 0.0;

            if (i == 1) {
                convertRate = NumberUtils.formatDouble(1.0 * targetPageSplitPv / startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble(1.0 * targetPageSplitPv / lastPageSplitPv, 2);
            }

            convertRateMap.put(targetPageSplit, convertRate);

            lastPageSplitPv = targetPageSplitPv;
        }

        return convertRateMap;
    }

    /**
     * 持久化页面切片转化率
     *
     * @param taskId         任务ID
     * @param convertRateMap 页面切片转化率
     */
    private static void persistConvertRate(Map<String, Double> convertRateMap, long taskId) {
        StringBuffer sb = new StringBuffer();

        for (Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();

            sb.append(pageSplit).append("=").append(convertRate).append("|");
        }

        String convertRate = sb.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        //封装
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskId(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDao pageSplitConvertRateDao = DaoFactory.getPageSplitConvertRateDaoImpl();
        pageSplitConvertRateDao.insert(pageSplitConvertRate);
    }
}
