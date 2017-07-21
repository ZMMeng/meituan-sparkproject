package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.SessionDetail;

/**
 * 抽取会话的详细信息的DAO接口
 * Created by Administrator on 2017/7/21.
 */
public interface ISessionDetailDao {

    /**
     * 向MySQL中插入抽取会话的详细信息
     *
     * @param sessionDetail 抽取会话的详细信息
     */
    void insert(SessionDetail sessionDetail);
}
