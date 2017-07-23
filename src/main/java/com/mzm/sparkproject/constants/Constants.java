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
    //会话ID
    String FIELD_SESSION_ID = "sessionId";
    //搜索关键词
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    //点击品类ID
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    //用户年龄
    String FIELD_AGE = "age";
    //用户职业
    String FIELD_PROFESSIONAL = "professional";
    //用户所在城市
    String FIELD_CITY = "city";
    //用户性别
    String FIELD_GENDER = "gender";
    //会话时长
    String FIELD_VISIT_LENGTH = "visitLength";
    //访问步长
    String FIELD_STEP_LENGTH = "stepLength";
    //会话开始时间
    String FIELD_START_TIME = "startTime";
    //品类ID
    String FIELD_CATEGORY_ID = "categoryId";
    //品类ID点击计数
    String FIELD_CLICK_COUNT = "clickCount";
    //品类ID下单计数
    String FIELD_ORDER_COUNT = "orderCount";
    //品类ID支付计数
    String FIELD_PAY_COUNT = "payCount";

    //抽取会话数量
    int EXTRACT_NUMBER = 100;

    //会话计数
    String SESSION_COUNT = "session_count";

    //时长在1~3s的会话计数
    String TIME_PERIOD_1s_3s = "1s_3s";
    //时长在4~6s的会话计数
    String TIME_PERIOD_4s_6s = "4s_6s";
    //时长在7~9s的会话计数
    String TIME_PERIOD_7s_9s = "7s_9s";
    //时长在10~30s的会话计数
    String TIME_PERIOD_10s_30s = "10s_30s";
    //时长在30~60s的会话计数
    String TIME_PERIOD_30s_60s = "30s_60s";
    //时长在1~3m的会话计数
    String TIME_PERIOD_1m_3m = "1m_3m";
    //时长在3~10m的会话计数
    String TIME_PERIOD_3m_10m = "3m_10m";
    //时长在10~30m的会话计数
    String TIME_PERIOD_10m_30m = "10m_30m";
    //时长在30m以上的会话计数
    String TIME_PERIOD_30m = "30m";

    //步长在1~3的会话计数
    String STEP_PERIOD_1_3 = "1_3";
    //步长在4~6的会话计数
    String STEP_PERIOD_4_6 = "4_6";
    //步长在7~9的会话计数
    String STEP_PERIOD_7_9 = "7_9";
    //步长在10~30的会话计数
    String STEP_PERIOD_10_30 = "10_30";
    //步长在30~60的会话计数
    String STEP_PERIOD_30_60 = "30_60";
    //步长在60以上的会话计数
    String STEP_PERIOD_60 = "60";

    /**
     * 任务相关常量
     */
    //开始日期
    String PARAM_START_DATE = "startDate";
    //结束日期
    String PARAM_END_DATE = "endDate";
    //年龄范围起始年龄
    String PARAM_START_AGE = "startAge";
    //年龄范围终止年龄
    String PARAM_END_AGE = "endAge";
    //职业范围
    String PARAM_PROFESSIONALS = "professionals";
    //城市范围
    String PARAM_CITIES = "cities";
    //性别范围
    String PARAM_GENDER = "gender";
    //搜索关键词
    String PARAM_KEYWORDS = "keywords";
    //品类ID
    String PARAM_CATEGORY_IDS = "categoryIds";

}
