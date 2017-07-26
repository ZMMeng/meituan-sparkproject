package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.AdStat;

import java.util.List;

/**
 * 每天各省各城市各支广告点击量的实时统计DAO
 * Created by Administrator on 2017/7/26.
 */
public interface IAdStatDao {

    /**
     * 向MySQL中插入每天各省各城市各支广告点击量的实时统计
     *
     * @param adStatList 每天各省各城市各支广告点击量的实时统计
     */
    void updateBatch(List<AdStat> adStatList);
}
