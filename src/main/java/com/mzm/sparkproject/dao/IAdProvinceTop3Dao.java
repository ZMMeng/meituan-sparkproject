package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.AdProvinceTop3;

import java.util.List;

/**
 * 每天各省Top3热门广告DAO
 * Created by Administrator on 2017/7/26.
 */
public interface IAdProvinceTop3Dao {

    /**
     * 批量更新每天各省Top3热门广告的统计结果
     *
     * @param adProvinceTop3List 每天各省Top3热门广告
     */
    void updateBatch(List<AdProvinceTop3> adProvinceTop3List);
}
