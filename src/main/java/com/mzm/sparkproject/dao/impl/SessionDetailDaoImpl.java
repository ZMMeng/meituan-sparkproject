package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ISessionDetailDao;
import com.mzm.sparkproject.domain.SessionDetail;
import com.mzm.sparkproject.jdbc.JdbcHelper;

/**
 * 抽取会话的详细信息的DAO实现类
 * Created by Administrator on 2017/7/21.
 */
public class SessionDetailDaoImpl implements ISessionDetailDao {

    /**
     * 向MySQL中插入抽取会话的详细信息
     *
     * @param sessionDetail 抽取会话的详细信息
     */
    @Override
    public void insert(SessionDetail sessionDetail) {

        String sql = "insert into session_detail values (?,?,?,?,?,?,?,?,?,?,?,?);";

        Object[] params = {sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        jdbcHelper.executeUpdate(sql, params);
    }
}
