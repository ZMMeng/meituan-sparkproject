package com.mzm.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.dao.IAreaTop3ProductDao;
import com.mzm.sparkproject.dao.ITaskDao;
import com.mzm.sparkproject.dao.factory.DaoFactory;
import com.mzm.sparkproject.domain.AreaTop3Product;
import com.mzm.sparkproject.domain.Task;
import com.mzm.sparkproject.utils.ParamUtils;
import com.mzm.sparkproject.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 各区域top3热门商品统计
 * Created by Administrator on 2017/7/25.
 */
public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        //创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(conf);

        //构建Spark上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //获取SQLContext
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //注册自定义函数
        sqlContext.udf().register("concat2", new Concat2UDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
        sqlContext.udf().register("get_json_obj", new GetJsonObjectUDF(), DataTypes.StringType);

        //生成模拟数据
        SparkUtils.mockDate(sc, sqlContext);

        //创建需要使用的DAO组件
        ITaskDao taskDao = DaoFactory.getTaskDaoImpl();
        //获取任务ID
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        //获取指定的任务
        Task task = taskDao.findById(taskId);
        if (task == null) {
            System.out.println(new Date() + "找不到相应的任务，任务ID为" + taskId);
            return;
        }
        //将任务参数封装成JSON格式数据
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //从任务参数中获取起始日期和结束日期
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        //根据任务参数，查询指定时间范围内的用户访问行为数据
        JavaRDD<Row> clickActionRDD = getClickActionRDDByDate(sqlContext, startDate, endDate);

        //将点击行为数据RDD转换成(城市ID,点击行为数据)的RDD
        JavaPairRDD<String, Row> cityId2ClickActionRDD = getCityId2ClickActionRDD(clickActionRDD);

        //从MySQL中查询城市信息
        JavaRDD<Row> cityInfoRDD = getCityInfoRDD(sqlContext);

        //将城市信息RDD转换成(城市ID,城市信息)的RDD
        JavaPairRDD<String, Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(cityInfoRDD);

        //生成点击商品基础信息临时表
        generateTempClickProductBasicTable(cityId2ClickActionRDD, cityId2CityInfoRDD, sqlContext);

        //生成各区域各商品点击次数的临时表
        generateTempAreaProductClickCountTable(sqlContext);

        //生成各区域各商品点击次数的临时表(包含商品的完整信息)
        generateTempAreaFullProductClickCountTable(sqlContext);

        //使用开窗函数获取各区域Top3热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        //由于数据量不大，所以直接将数据collect()到本地
        List<Row> areaTop3ProductList = areaTop3ProductRDD.collect();

        //批量插入MySQL数据库
        persistAreaTop3Product(areaTop3ProductList, taskId);

        //关闭上下文
        sc.close();
    }

    /**
     * 查询指定日期范围内的点击行为数据
     *
     * @param sqlContext Spark SQL Context
     * @param startDate  用户指定的起始日期
     * @param endDate    用户指定的结束日期
     * @return 指定日期范围内的点击行为数据的RDD
     */
    private static JavaRDD<Row> getClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate) {

        String sql = "SELECT `city_id`,`click_product_id product_id` FROM user_visit_action WHERE `click_product_id` " +
                "IS NOT NULL AND `click_product_id` != 'NULL' AND `click_product_id` != 'null' AND `action_time` >= " +
                "'" + startDate + "' AND `action_time` <= '" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);
        return clickActionDF.javaRDD();
    }

    /**
     * 将点击行为数据RDD转换成(城市ID,点击行为数据)的RDD
     *
     * @param clickActionRDD 点击行为数据RDD
     * @return (城市ID, 点击行为数据)的RDD
     */
    private static JavaPairRDD<String, Row> getCityId2ClickActionRDD(JavaRDD<Row> clickActionRDD) {
        return clickActionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String cityId = row.getString(12);
                return new Tuple2<>(cityId, row);
            }
        });
    }

    /**
     * 从MySQL中查询城市信息
     *
     * @param sqlContext SQL Context
     * @return 城市信息RDD
     */
    private static JavaRDD<Row> getCityInfoRDD(SQLContext sqlContext) {

        //构建MySQL连接配置信息
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);

        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("dbtable", "city_info");

        //通过SQL Context从MySQL中查询城市相关信息数据
        DataFrame cityInfoDf = sqlContext.read().format("jdbc").options(options).load();
        return cityInfoDf.javaRDD();
    }

    /**
     * 将城市信息RDD转换成(城市ID,城市信息)的RDD
     *
     * @param cityInfoRDD 城市信息RDD
     * @return (城市ID, 城市信息)的RDD
     */
    private static JavaPairRDD<String, Row> getCityId2CityInfoRDD(JavaRDD<Row> cityInfoRDD) {
        return cityInfoRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String cityId = row.getString(0);
                return new Tuple2<>(cityId, row);
            }
        });
    }

    /**
     * 生成点击商品基础信息临时表
     *
     * @param cityId2ClickActionRDD (城市ID,点击行为数据)的RDD
     * @param cityId2CityInfoRDD    (城市ID,城市信息)的RDD
     * @param sqlContext            Spark SQL Context
     */
    private static void generateTempClickProductBasicTable(JavaPairRDD<String, Row> cityId2ClickActionRDD,
                                                           JavaPairRDD<String, Row> cityId2CityInfoRDD, SQLContext
                                                                   sqlContext) {

        //关联
        JavaPairRDD<String, Tuple2<Row, Row>> joinRDD = cityId2ClickActionRDD.join(cityId2CityInfoRDD);

        //转化
        JavaRDD<Row> mappedRDD = joinRDD.map(new Function<Tuple2<String, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Row, Row>> tuple) throws Exception {

                String cityId = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._2;

                //从点击行为数据中获取点击的产品ID
                String productId = clickAction.getString(1);
                //从城市信息中获取城市名称和所属区域
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(cityId, cityName, area, productId);
            }
        });

        //基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);

        //将DataFrame中的数据，注册成临时表(tmp_click_product_basic)
        df.registerTempTable("tmp_clk_prod_basic");

    }

    /**
     * 生成各区域各商品点击次数的临时表
     *
     * @param sqlContext Spark SQL Context
     */
    private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {

        //按照area和product_id进行分组，得到每个区域下每个商品ID的城市拼接信息，从而计算出点击次数
        String sql = "SELECT `area`,`product_id`,group_concat_distinct(concat2(city_id,city_name,':') city_infos," +
                "count(1) click_count FROM tmp_clk_prod_basic GROUP BY `area`,`product_id`;";

        DataFrame df = sqlContext.sql(sql);

        //再次将查询出来的数据注册成为一个临时表，存储各区域各商品的点击次数
        df.registerTempTable("tmp_area_prod_clk_cnt");
    }

    /**
     * 生成各区域各商品点击次数的临时表(包含商品的完整信息)
     *
     * @param sqlContext Spark SQL Context
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        String sql = "SELECT `tapcc.area`,`tapcc.product_id`,`tapcc.click_count`,`tapcc.city_infos`,`click_count`," +
                "`product_name`,get_json_obj(extend_info) FROM tmp_area_prod_clk_cnt tapcc JOIN product_info pi " +
                "ON tapcc.product_id=pi.product_id;";

        DataFrame df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_fullprod_clk_cnt");
    }

    /**
     * 获取各区域Top3热门商品
     *
     * @param sqlContext Spark SQL Context
     * @return 各区域Top3热门商品的RDD
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {

        String sql = "SELECT `area`,CASE WHEN `area`='华北' OR `area`='华东' THEN 'A级' WHEN `area`='华南' OR `area`='华中' " +
                "THEN 'B级' WHEN `area`='西北' OR `area`='西南' THEN 'C级' ELSE 'D级' END area_level,`product_id`," +
                "`click_count`,`city_infos`,`product_name`,`product_status` FROM (SELECT `area`,`product_id`,`click_count`," +
                "`city_infos`,`product_name`,`product_status`,ROW_NUMBER() OVER(PARTITION BY `area` ORDER BY `click_count` DESC) rank FROM " +
                "tmp_area_fullprod_clk_cnt) t WHERE `rank` <= 3;";

        DataFrame df = sqlContext.sql(sql);
        return df.javaRDD();
    }

    /**
     * 将计算出来的各区域Top3热门商品插入MySQL
     *
     * @param rows 各区域Top3热门商品
     * @param taskId 任务ID
     */
    private static void persistAreaTop3Product(List<Row> rows, long taskId){

        List<AreaTop3Product> areaTop3ProductList = new ArrayList<>();

        for(Row row : rows){
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskId(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductId(Long.valueOf(row.getString(2)));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));

            areaTop3ProductList.add(areaTop3Product);
        }

        IAreaTop3ProductDao areaTop3ProductDao = DaoFactory.getAreaTop3ProductDaoImpl();

        areaTop3ProductDao.insertBatch(areaTop3ProductList);

    }
}
