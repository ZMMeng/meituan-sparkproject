package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.AdUserClickCount;

import java.util.List;

/**
 * 用户广告点击量DAO接口
 * Created by Administrator on 2017/7/26.
 */
public interface IAdUserClickCountDao {

    /**
     * 向MySQL中插入用户广告点击量的统计结果
     *
     * @param adUserClickCountList 用户广告点击量的统计结果
     */
    void updateBatch(List<AdUserClickCount> adUserClickCountList);

    /**
     * 在MySQL中根据多个key查询用户广告点击量
     *
     * @param date   日期
     * @param userId 用户ID
     * @param adId   广告ID
     * @return 用户广告点击量
     */
    long findClickCountByMultiKey(String date, long userId, long adId);
}
