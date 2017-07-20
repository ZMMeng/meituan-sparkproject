package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.SessionAggrStat;

/**
 * 会话统计模块DAO接口
 * Created by Administrator on 2017/7/20.
 */
public interface ISessionAggrStatDao {

    /**
     * 向MySQL插入会话聚合统计结果
     * @param sessionAggrStat 会话聚合统计结果
     */
    void insert(SessionAggrStat sessionAggrStat);
}
