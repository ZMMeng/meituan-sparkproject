package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ITop10SessionDao;
import com.mzm.sparkproject.domain.Top10Session;
import com.mzm.sparkproject.jdbc.JdbcHelper;

/**
 * top10活跃session的DAO实现类
 * Created by 蒙卓明 on 2017/7/22.
 */
public class Top10SessionDaoImpl implements ITop10SessionDao {

    /**
     * 向MySQL中插入top10活跃session的统计结果
     *
     * @param top10Session top10活跃session的统计结果
     */
    @Override
    public void insert(Top10Session top10Session) {

        String sql = "insert into top10_session values (?,?,?,?);";

        Object[] params = {top10Session.getTaskId(),
                top10Session.getCategoryId(),
                top10Session.getSessionId(),
                top10Session.getClickCount()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
