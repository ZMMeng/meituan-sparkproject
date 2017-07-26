package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.AdClickTrend;

import java.util.List;

/**
 * 最近一小时广告点击趋势DAO
 * Created by Administrator on 2017/7/26.
 */
public interface IAdClickTrendDao {

    /**
     * 向MySQL中更新最近一小时广告点击趋势
     *
     * @param adClickTrendList 最近一小时广告点击趋势
     */
    void updateBatch(List<AdClickTrend> adClickTrendList);
}
