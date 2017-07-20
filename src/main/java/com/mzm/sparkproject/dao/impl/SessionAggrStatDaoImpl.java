package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ISessionAggrStatDao;
import com.mzm.sparkproject.domain.SessionAggrStat;
import com.mzm.sparkproject.jdbc.JdbcHelper;

/**
 * Created by Administrator on 2017/7/20.
 */
public class SessionAggrStatDaoImpl implements ISessionAggrStatDao {

    /**
     * 向MySQL插入会话聚合统计结果
     *
     * @param sessionAggrStat 会话聚合统计结果
     */
    @Override
    public void insert(SessionAggrStat sessionAggrStat) {
        String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        Object[] params = {sessionAggrStat.getTaskId(),
                sessionAggrStat.getSessionCount(),
                sessionAggrStat.getTimePeriod1sTo3sRatio(),
                sessionAggrStat.getTimePeriod4sTo6sRatio(),
                sessionAggrStat.getTimePeriod7sTo9sRatio(),
                sessionAggrStat.getTimePeriod10sTo30sRatio(),
                sessionAggrStat.getTimePeriod30sTo60sRatio(),
                sessionAggrStat.getTimePeriod1mTo3mRatio(),
                sessionAggrStat.getTimePeriod3mTo10mRatio(),
                sessionAggrStat.getTimePeriod10mTo30mRatio(),
                sessionAggrStat.getTimePeriod30mRatio(),
                sessionAggrStat.getStepLength1To3Ratio(),
                sessionAggrStat.getStepLength4To6Ratio(),
                sessionAggrStat.getStepLength7To9Ratio(),
                sessionAggrStat.getStepLength10To30Ratio(),
                sessionAggrStat.getStepLength30To60Ratio(),
                sessionAggrStat.getStepLength60Ratio()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
