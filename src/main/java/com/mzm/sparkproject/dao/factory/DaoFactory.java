package com.mzm.sparkproject.dao.factory;

import com.mzm.sparkproject.dao.*;
import com.mzm.sparkproject.dao.impl.*;

/**
 * Dao工厂类
 * Created by Administrator on 2017/7/19.
 */
public class DaoFactory {

    /**
     * 获取任务管理DAO
     *
     * @return 任务管理DAO的实现类对象
     */
    public static ITaskDao getTaskDaoImpl() {
        return new TaskDaoImpl();
    }

    /**
     * 获取会话聚合统计DAO
     *
     * @return 会话聚合统计DAO的实现类对象
     */
    public static ISessionAggrStatDao getSessionAggrStatDaoImpl() {
        return new SessionAggrStatDaoImpl();
    }

    /**
     * 获取抽取会话DAO
     *
     * @return 抽取会话DAO的实现类对象
     */
    public static ISessionRandomExtractDao getSessionRandomExtractDaoImpl() {
        return new SessionRandomExtractDaoImpl();
    }

    /**
     * 获取抽取会话详细信息DAO
     *
     * @return 抽取会话详细信息DAO的实现类对象
     */
    public static ISessionDetailDao getSessionDetailDaoImpl() {
        return new SessionDetailDaoImpl();
    }

    /**
     * 获取Top10热门品类DAO
     *
     * @return Top10热门品类DAO的实现类对象
     */
    public static ITop10CategoryDao getTop10CategoryDaoImpl() {
        return new Top10CategoryDaoImpl();
    }

    /**
     * 获取Top10活跃session的DAO
     *
     * @return Top10活跃session的DAO的实现类
     */
    public static ITop10SessionDao getTop10SessionDaoImpl() {
        return new Top10SessionDaoImpl();
    }

    /**
     * 获取页面切片转化率的DAO
     *
     * @return 获取页面切片转化率的DAO的实现类
     */
    public static IPageSplitConvertRateDao getPageSplitConvertRateDaoImpl() {
        return new PageSplitConvertRateDaoImpl();
    }

    /**
     * 获取各区域Top3热门商品的DAO
     *
     * @return 各区域Top3热门商品的DAO的实现类
     */
    public static IAreaTop3ProductDao getAreaTop3ProductDaoImpl() {
        return new AreaTop3ProductListDaoImpl();
    }

    /**
     * 获取用户广告点击量的DAO
     *
     * @return 用户广告点击量的DAO的实现类
     */
    public static IAdUserClickCountDao getAdUserClickCountDaoImpl() {
        return new AdUserClickCountDaoImpl();
    }

    /**
     * 获取广告点击量黑名单用户的DAO
     *
     * @return 广告点击量黑名单用户的DAO的实现类
     */
    public static IAdBlackListDao getAdBlackListDaoImpl() {
        return new AdBlackListDaoImpl();
    }

    /**
     * 获取天各省各城市各支广告点击量的实时统计DAO
     *
     * @return 天各省各城市各支广告点击量的实时统计DAO的实现类
     */
    public static IAdStatDao getAdStatDaoImpl() {
        return new AdStatDaoImpl();
    }

    /**
     * 获取每天各省Top3热门广告DAO
     *
     * @return 每天各省Top3热门广告DAO的实现类
     */
    public static IAdProvinceTop3Dao getAdProvinceTop3DaoImpl() {
        return new AdProvinceTop3DaoImpl();
    }

    /**
     * 获取最近一小时内广告点击量统计结果的DAO
     *
     * @return 最近一小时内广告点击量统计结果的DAO的实现类
     */
    public static IAdClickTrendDao getAdClickTrendDaoImpl() {
        return new AdClickTrendDaoImpl();
    }
}
