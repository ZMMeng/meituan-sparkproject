package com.mzm.sparkproject.spark.ad;

import com.google.common.base.Optional;

import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.*;
import com.mzm.sparkproject.dao.factory.DaoFactory;
import com.mzm.sparkproject.domain.*;
import com.mzm.sparkproject.utils.DateUtils;
import com.mzm.sparkproject.utils.SparkUtils;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计spark
 * Created by 蒙卓明 on 2017/7/25.
 */
public class AdClickRealTimeStatSpark {

    public static void main(String[] args) {
        //构建Spark Streaming的上下文对象
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_AD);

        SparkUtils.setMaster(conf);

        //Spark Streaming上下文，第一个参数是配置，第二个参数是实时处理batch的interval
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Duration.apply(5));

        //构建Kafka参数Map
        Map<String, String> kafkaParams = new HashMap<>();
        //将Kafka集群的主机名和端口号放入Kafka参数Map
        kafkaParams.put(Constants.KAFKA_META_DATA_BROKER_LIST,
                ConfigurationManager.getProperty(Constants.KAFKA_META_DATA_BROKER_LIST));

        //构建Topic Set
        //获取Kafka的topics
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<>();
        topics.addAll(Arrays.asList(kafkaTopicsSplited));

        //基于Kafka direct API模式，创建针对Kafka指定topic数据来源的输入DStream(离散流)
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        //根据mysql中的动态黑名单，进行实时的黑名单过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlackList(adRealTimeLogDStream);

        //生成动态黑名单
        generateDynamicBlackList(adRealTimeLogDStream);

        //实时统计每天各省各城市各个广告的点击量
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

        //实时统计每天各省Top3热门广告
        calculateProvinceTop3Ads(adRealTimeStatDStream);

        //实时统计每天每个广告在最近1h内的滑动窗口内的点击趋势(每分钟的点击量)
        calculateAdClickCountByWindow(filteredAdRealTimeLogDStream);

        //启动上下文
        jssc.start();

        //等待执行结束
        jssc.awaitTermination();

        //关闭上下文
        jssc.close();
    }

    /**
     * 根据mysql中的动态黑名单，进行实时的黑名单过滤
     *
     * @param adRealTimeLogDStream 原始的每天各用户对各个广告的点击量
     * @return 过滤后的每天各用户对各个广告的点击量
     */
    private static JavaPairDStream<String, String> filterByBlackList(JavaPairInputDStream<String, String>
                                                                             adRealTimeLogDStream) {
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {

                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {

                        //从MySQL中查询所有黑名单用户
                        IAdBlackListDao adBlackListDao = DaoFactory.getAdBlackListDaoImpl();
                        List<AdBlackList> adBlackLists = adBlackListDao.findAll();

                        //将所有黑名单用户转换成RDD
                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
                        for (AdBlackList adBlackList : adBlackLists) {
                            tuples.add(new Tuple2<>(adBlackList.getUserId(), true));
                        }
                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blackListRDD = sc.parallelizePairs(tuples);

                        //将实时日志RDD映射成(用户ID,实时日志)
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                                    @Override
                                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple)
                                            throws Exception {
                                        //从tuple中获取每一条原始的实时日志
                                        String log = tuple._2;
                                        //对log进行切分
                                        String[] logSplited = log.split(" ");

                                        //提取用户ID
                                        long userId = Long.valueOf(logSplited[3]);

                                        return new Tuple2<>(userId, tuple);
                                    }
                                });

                        //将(用户ID,实时日志)RDD与黑名单用户RDD进行左外连接
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD
                                .leftOuterJoin(blackListRDD);

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD
                                .filter(
                                        new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>,
                                                Boolean>() {

                                            @Override
                                            public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>,
                                                    Optional<Boolean>>> tuple) throws Exception {
                                                Optional<Boolean> optional = tuple._2._2;

                                                if (optional.isPresent() && optional.get()) {
                                                    //说明实施日志与某个黑名单用户连接，将该条记录过滤
                                                    return false;
                                                }
                                                return true;
                                            }
                                        });

                        //将过滤后的RDD映射回原来的实时日志类型
                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>,
                                        String, String>() {

                                    @Override
                                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>,
                                            Optional<Boolean>>> tuple) throws Exception {
                                        return tuple._2._1;
                                    }
                                });

                        return resultRDD;
                    }
                });

        return filteredAdRealTimeLogDStream;
    }

    /**
     * 生成动态黑名单
     *
     * @param adRealTimeLogDStream 每天各用户对各个广告的点击量
     */
    private static void generateDynamicBlackList(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        //计算每5s的batch，每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        //从tuple中获取每一条原始的实时日志
                        String log = tuple._2;
                        //对log进行切分
                        String[] logSplited = log.split(" ");

                        //提取出日期(yyyyMMdd)，用户ID以及广告ID
                        String timestamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String dateKey = DateUtils.formatDateKey(date);

                        long userId = Long.valueOf(logSplited[3]);

                        long adId = Long.valueOf(logSplited[4]);

                        //拼接key
                        String key = dateKey + "_" + userId + "_" + adId;

                        return new Tuple2<>(key, 1L);
                    }
                });

        //针对处理后的日志格式，执行reduceByKey，获取每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        //向MySQL中插入每天各用户对各个广告的点击量
        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdUserClickCount> adUserClickCounts = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                            long userId = Long.valueOf(keySplited[1]);
                            long adId = Long.valueOf(keySplited[2]);
                            long clickCount = tuple._2;

                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                            adUserClickCount.setDate(date);
                            adUserClickCount.setUserId(userId);
                            adUserClickCount.setAdId(adId);
                            adUserClickCount.setClickCount(clickCount);

                            adUserClickCounts.add(adUserClickCount);
                        }

                        IAdUserClickCountDao adUserClickCountDao = DaoFactory.getAdUserClickCountDaoImpl();
                        adUserClickCountDao.updateBatch(adUserClickCounts);
                    }
                });
                return null;
            }
        });

        //从每天各用户对各个广告的点击量，过滤出单个用户对单个广告的点击量超过100的记录，将该用户视为黑名单用户
        JavaPairDStream<String, Long> blackListDStream = dailyUserAdClickCountDStream.filter(
                new Function<Tuple2<String, Long>, Boolean>() {

                    @Override
                    public Boolean call(Tuple2<String, Long> tuple) throws Exception {

                        String key = tuple._1;
                        String[] keySplited = key.split("_");

                        String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                        long userId = Long.valueOf(keySplited[1]);
                        long adId = Long.valueOf(keySplited[2]);

                        //从MySQL中查询指定用户对指定广告的点击量
                        IAdUserClickCountDao adUserClickCountDao = DaoFactory.getAdUserClickCountDaoImpl();
                        long clickCount = adUserClickCountDao.findClickCountByMultiKey(date, userId, adId);
                        if (clickCount >= 100) {
                            return true;
                        }
                        return false;
                    }
                });

        //对黑名单用户的点击记录中提取该用户ID
        JavaDStream<Long> blackListUserIdDStream = blackListDStream.map(new Function<Tuple2<String, Long>, Long>() {

            @Override
            public Long call(Tuple2<String, Long> tuple) throws Exception {
                String key = tuple._1;
                String[] keySplited = key.split("_");
                long userId = Long.valueOf(keySplited[1]);
                return userId;
            }
        });

        //对黑名单用户进行去重
        JavaDStream<Long> distinctBlackListUserDStream = blackListUserIdDStream.transform(
                new Function<JavaRDD<Long>, JavaRDD<Long>>() {
                    @Override
                    public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                        return rdd.distinct();
                    }
                });

        //将黑名单用户ID插入到MySQL中
        distinctBlackListUserDStream.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
            public void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlackList> adBlackLists = new ArrayList<>();
                        while (iterator.hasNext()) {
                            long userId = iterator.next();

                            AdBlackList adBlackList = new AdBlackList();
                            adBlackList.setUserId(userId);

                            adBlackLists.add(adBlackList);
                        }

                        IAdBlackListDao adBlackListDao = DaoFactory.getAdBlackListDaoImpl();
                        adBlackListDao.insertBatch(adBlackLists);
                    }
                });
            }
        });
    }

    /**
     * 计算每天各省各城市各个广告的点击量
     *
     * @param filteredAdRealTimeLogDStream 过滤后的广告点击数据
     * @return 每天各省各城市各个广告的点击量的统计结果
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String>
                                                                               filteredAdRealTimeLogDStream) {
        //将过滤后的广告点击数据映射成(date_province_city_ad,1)的形式
        JavaPairDStream<String, Long> dateProvinceCityAdLogDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        //从过滤后的广告点击数据中，获取日期(yyyyMMdd)、省份、城市和广告ID
                        String timestamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String datekey = DateUtils.formatDateKey(date);

                        String province = logSplited[1];
                        String city = logSplited[2];
                        long adId = Long.valueOf(logSplited[4]);

                        //拼接：日期_省份_城市_广告ID
                        String key = new StringBuffer().append(datekey).append("_").append(province).append("_")
                                .append(city).append("_").append(adId).toString();
                        return new Tuple2<>(key, 1L);
                    }
                });

        //
        JavaPairDStream<String, Long> aggregatedDStream = dateProvinceCityAdLogDStream.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

                    /**
                     * 对每个batch rdd累加相应key的所有值
                     *
                     * @param values   batch rdd中，每个key对应的值
                     * @param optional 该key的状态
                     * @return 该key对应的值的总和
                     * @throws Exception
                     */
                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                        //根据optional判断该key的状态，此时是第一个batch rdd的相应key
                        long clickCount = 0L;

                        if (optional.isPresent()) {
                            //其他batch rdd的相应key，即之前已计算过另外一些batch rdd的相应key的计算
                            clickCount = optional.get();
                        }

                        //遍历所有的values，进行累加
                        for (Long value : values) {
                            clickCount += value;
                        }
                        return Optional.of(clickCount);
                    }
                });

        //将每天各省各城市各支广告点击量的实时统计结果插入MySQL中
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {

                        List<AdStat> adStatList = new ArrayList<>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String key = tuple._1;
                            String[] keySplited = key.split("_");

                            String date = keySplited[0];
                            String province = keySplited[1];
                            String city = keySplited[2];
                            long adId = Long.valueOf(keySplited[3]);

                            long clickCount = tuple._2;

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdId(adId);
                            adStat.setClickCount(clickCount);

                            adStatList.add(adStat);
                        }

                        IAdStatDao adStatDao = DaoFactory.getAdStatDaoImpl();
                        adStatDao.updateBatch(adStatList);
                    }
                });
                return null;
            }
        });

        return aggregatedDStream;
    }

    /**
     * 计算每天各省份的top3热门广告
     *
     * @param adRealTimeStatDStream 每天各省份各城市各广告的点击量
     */
    private static void calculateProvinceTop3Ads(JavaPairDStream<String, Long> adRealTimeStatDStream) {

        JavaDStream<Row> rowDStream = adRealTimeStatDStream.transform(
                new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {

                    @Override
                    public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                        //每一个Batch RDD代表了最新的全量的每天各省份各城市各广告的点击量

                        //将每天各省份各城市各广告的点击量RDD映射成每天各省份各广告的点击量RDD
                        //(date_province_city_adId,clickCount) -> (date_province_adId,clickCount)
                        JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, Long>, String, Long>() {

                                    @Override
                                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {

                                        String key = tuple._1;

                                        //从key中获取日期、省份和广告ID
                                        String[] keySplited = key.split("_");
                                        String date = keySplited[0];
                                        String province = keySplited[1];
                                        long adId = Long.valueOf(keySplited[2]);

                                        //获取点击量
                                        long clickCount = tuple._2;

                                        String newKey = new StringBuffer().append(date).append("_").append(province)
                                                .append("_").append(adId).toString();
                                        return new Tuple2<>(newKey, clickCount);
                                    }
                                });

                        JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(
                                new Function2<Long, Long, Long>() {
                                    @Override
                                    public Long call(Long v1, Long v2) throws Exception {
                                        return v1 + v2;
                                    }
                                });

                        //将dailyAdClickCountByProvinceRDD转换为DataFrame
                        JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(
                                new Function<Tuple2<String, Long>, Row>() {

                                    @Override
                                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                                        String key = tuple._1;
                                        String[] keySplited = key.split("_");

                                        //从key中获取日期、省份和广告ID
                                        String dateKey = keySplited[0];
                                        String province = keySplited[1];
                                        long adId = Long.valueOf(keySplited[2]);

                                        String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));

                                        //获取点击量
                                        long clickCount = tuple._2;
                                        return RowFactory.create(date, province, String.valueOf(adId), clickCount);
                                    }
                                });

                        StructType schema = DataTypes.createStructType(Arrays.asList(
                                DataTypes.createStructField("date", DataTypes.StringType, true),
                                DataTypes.createStructField("province", DataTypes.StringType, true),
                                DataTypes.createStructField("ad_id", DataTypes.StringType, true),
                                DataTypes.createStructField("click_count", DataTypes.LongType, true))
                        );

                        HiveContext sqlContext = new HiveContext(rdd.context());

                        DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);

                        //注册成临时表
                        dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_clk_count_by_prov");

                        //使用Spark SQL执行SQL，配合开窗函数，统计出各省份Top3热门广告
                        String sql = "SELECT `date`,`province`,`ad_id`,`click_count` FROM (SELECT `date`,`province`," +
                                "`ad_id`,`click_count`,ROW NUMBER() OVER(PARTITION BY `province` ORDER BY " +
                                "`click_count` DESC) rank FROM tmp_daily_ad_clk_count_by_prov) t WHERE" +
                                " `rank`>=3;";

                        DataFrame provinceTop3AdDF = sqlContext.sql(sql);

                        return provinceTop3AdDF.javaRDD();
                    }
                });

        //将统计出来的每天各省Top3热门广告信息插入MySQL中
        rowDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
            @Override
            public Void call(JavaRDD<Row> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row>iterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3List = new ArrayList<>();

                        while(iterator.hasNext()){
                            Row row = iterator.next();
                            //获取日期、省份、广告ID和点击量
                            String date = row.getString(1);
                            String province = row.getString(2);
                            long adId = Long.valueOf(row.getString(3));
                            long clickCount = row.getLong(4);

                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdId(adId);
                            adProvinceTop3.setClickCount(clickCount);

                            adProvinceTop3List.add(adProvinceTop3);
                        }

                        IAdProvinceTop3Dao adProvinceTop3Dao = DaoFactory.getAdProvinceTop3DaoImpl();
                        adProvinceTop3Dao.updateBatch(adProvinceTop3List);
                    }
                });
                return null;
            }
        });
    }

    /**
     * 计算最近一小时广告的点击量
     *
     * @param filteredAdRealTimeLogDStream 过滤后的广告点击数据
     */
    private static void calculateAdClickCountByWindow(JavaPairDStream<String, String> filteredAdRealTimeLogDStream){

        //将过滤后的广告点击数据，映射成(时间(到分钟)_广告ID,1)的形式
        JavaPairDStream<String, Long> pairDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {

                        String[] logSplited = tuple._2.split("_");
                        String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
                        long adId = Long.valueOf(logSplited[4]);
                        return new Tuple2<>(timeMinute + "_" + adId, 1L);
                    }
                });

        //每次出来一个新的batch(10s一次)，获取最近一小时内的所有batch，通过reduceByKey，获取最近一小时广告点击量
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
        }, Durations.minutes(60), Durations.seconds(10));

        aggrRDD.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdClickTrend> adClickTrendList = new ArrayList<>();

                        while(iterator.hasNext()){
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String dateMinute = keySplited[0];
                            long adId = Long.valueOf(keySplited[1]);
                            long clickCount = tuple._2;
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdId(adId);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrendList.add(adClickTrend);
                        }

                        IAdClickTrendDao adClickTrendDao = DaoFactory.getAdClickTrendDaoImpl();
                        adClickTrendDao.updateBatch(adClickTrendList);
                    }
                });
                return null;
            }
        });
    }

}
