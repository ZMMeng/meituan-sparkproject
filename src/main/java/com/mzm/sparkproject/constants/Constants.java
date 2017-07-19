package com.mzm.sparkproject.constants;

/**
 * 常量接口
 * Created by Administrator on 2017/7/19.
 */
public interface Constants {

    /**
     * 项目配置
     */
    //驱动
    String JDBC_DRIVER = "jdbc.driver";
    //数据库连接池大小
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    //数据库连接URL
    String JDBC_URL = "jdbc.url";
    //数据库连接用户名
    String JDBC_USERNAME = "jdbc.username";
    //数据库连接密码
    String JDBC_PASSWORD = "jdbc.password";
    //Spark是否本地运行
    String SPARK_LOCAL = "spark.local";

    /**
     * Spark作业相关常量
     */
    //session分析作业名称
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";

    /**
     * 任务相关常量
     */
    //开始日期
    String PARAM_START_DATE = "startDate";
    //结束日期
    String PARAM_END_DATE = "endDate";
}
