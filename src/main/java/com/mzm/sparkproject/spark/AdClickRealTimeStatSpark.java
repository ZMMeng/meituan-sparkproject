package com.mzm.sparkproject.spark;

import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.utils.DateUtils;
import com.mzm.sparkproject.utils.SparkUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
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

    public static void main(String[] args){
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

        //计算每5s的batch，每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String,String>, String, Long>() {
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



        //启动上下文
        jssc.start();

        //等待执行结束
        jssc.awaitTermination();

        //关闭上下文
        jssc.close();
    }
}
