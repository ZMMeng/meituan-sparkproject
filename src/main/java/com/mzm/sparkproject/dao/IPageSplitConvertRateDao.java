package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.PageSplitConvertRate;

/**
 * 页面切片转化率DAO接口
 * Created by 蒙卓明 on 2017/7/24.
 */
public interface IPageSplitConvertRateDao {

    /**
     * 向MySQL中插入页面切片转化率的统计结果
     * @param pageSplitConvertRate 页面切片转化率的统计结果
     */
    void insert(PageSplitConvertRate pageSplitConvertRate);
}
