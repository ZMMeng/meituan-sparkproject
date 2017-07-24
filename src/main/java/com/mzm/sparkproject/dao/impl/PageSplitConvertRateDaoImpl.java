package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IPageSplitConvertRateDao;
import com.mzm.sparkproject.domain.PageSplitConvertRate;
import com.mzm.sparkproject.jdbc.JdbcHelper;

/**
 * 页面切片转化率DAO实现类
 * Created by 蒙卓明 on 2017/7/24.
 */
public class PageSplitConvertRateDaoImpl implements IPageSplitConvertRateDao{

    /**
     * 向MySQL中插入页面切片转化率的统计结果
     *
     * @param pageSplitConvertRate 页面切片转化率的统计结果
     */
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {

        String sql = "insert into page_split_convert_rate values (?,?);";

        Object[] params = {pageSplitConvertRate.getTaskId(), pageSplitConvertRate.getConvertRate()};

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
