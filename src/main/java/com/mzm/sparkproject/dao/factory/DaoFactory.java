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
    public static ISessionDetailDao getSessionDetailDaoImpl(){
        return new SessionDetailDaoImpl();
    }

    /**
     * 获取Top10热门品类DAO
     *
     * @return Top10热门品类DAO的实现类对象
     */
    public static ITop10CategoryDao getTop10CategoryDaoImpl(){
        return new Top10CategoryDaoImpl();
    }

    /**
     * 获取Top10活跃session的DAO
     *
     * @return Top10活跃session的DAO的实现类
     */
    public static ITop10SessionDao getTop10SessionDaoImpl(){
        return new Top10SessionDaoImpl();
    }
}
