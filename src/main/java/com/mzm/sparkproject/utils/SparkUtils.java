package com.mzm.sparkproject.utils;

import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark相关的工具方法
 * Created by Administrator on 2017/7/24.
 */
public class SparkUtils {

    /**
     * 依据当前是否本地测试的配置，决定如何设置SparkConf的Master
     * @param conf SparkConf
     */
    public static void setMaster(SparkConf conf){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            //是本地模式
            conf.setMaster("local");
        }
    }

    /**
     * 生成模拟数据(本地模式生效)
     *
     * @param sc         Spark Context对象
     * @param sqlContext SQL Context
     */
    public static void mockDate(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取SQL Context
     *
     * @param sc Spark Context对象
     * @return SQL Context
     */
    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        }
        return new HiveContext(sc);
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQL Context
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {

        //从JSON数据中获取起始日期和结束日期
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where date>='" + startDate + "' and date<'" + endDate
                + "'";
        DataFrame actionDf = sqlContext.sql(sql);

        return actionDf.javaRDD();
    }

}
