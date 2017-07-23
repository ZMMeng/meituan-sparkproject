package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.Top10Session;

/**
 * top10活跃session的DAO
 * Created by 蒙卓明 on 2017/7/22.
 */
public interface ITop10SessionDao {

    /**
     * 向MySQL中插入top10活跃session的统计结果
     *
     * @param top10Session top10活跃session的统计结果
     */
    void insert(Top10Session top10Session);
}
