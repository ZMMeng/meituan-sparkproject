package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ISessionAggrStatDao;
import com.mzm.sparkproject.dao.ITaskDao;

/**
 * Dao工厂类
 * Created by Administrator on 2017/7/19.
 */
public class DaoFactory {

    /**
     * 获取任务管理DAO
     *
     * @return 任务管理DAO的实现类
     */
    public static ITaskDao getTaskDaoImpl() {
        return new TaskDaoImpl();
    }

    /**
     * 获取会话聚合统计DAO
     *
     * @return 会话聚合统计DAO的实现类
     */
    public static ISessionAggrStatDao getSessionAggrStatDaoImpl(){
        return new SessionAggrStatDaoImpl();
    }
}
