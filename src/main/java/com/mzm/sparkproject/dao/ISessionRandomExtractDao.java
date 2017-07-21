package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块的接口
 * Created by Administrator on 2017/7/21.
 */
public interface ISessionRandomExtractDao {

    /**
     * 向MySQL中插入随机抽取的session信息
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}
