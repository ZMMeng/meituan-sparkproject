package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.Top10Category;

/**
 * Top10品类DAO接口
 * Created by 蒙卓明 on 2017/7/22.
 */
public interface ITop10CategoryDao {

    /**
     * 向MySQL中插入Top10品类的统计结果
     *
     * @param top10Category Top10品类
     */
    void insert(Top10Category top10Category);
}
