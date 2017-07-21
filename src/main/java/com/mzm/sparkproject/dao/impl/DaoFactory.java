package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ISessionAggrStatDao;
import com.mzm.sparkproject.dao.ISessionDetailDao;
import com.mzm.sparkproject.dao.ISessionRandomExtractDao;
import com.mzm.sparkproject.dao.ITaskDao;

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
}
