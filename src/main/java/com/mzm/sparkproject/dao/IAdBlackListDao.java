package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.AdBlackList;

import java.util.List;

/**
 * 广告黑名单用户DAO接口
 * Created by Administrator on 2017/7/26.
 */
public interface IAdBlackListDao {

    /**
     * 批量向MySQL插入广告黑名单用户
     *
     * @param adBlackListList 广告黑名单用户
     */
    void insertBatch(List<AdBlackList> adBlackListList);

    /**
     * 在MySQL中查询所有广告黑名单用户
     *
     * @return 所有广告黑名单用户
     */
    List<AdBlackList> findAll();
}
