package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.AreaTop3Product;

import java.util.List;

/**
 * 各区域Top3热门商品DAO接口
 * Created by Administrator on 2017/7/25.
 */
public interface IAreaTop3ProductDao {

    /**
     * 将各区域Top3热门商品批量插入MySQL中
     *
     * @param areaTop3ProductList 各区域Top3热门商品
     */
    void insertBatch(List<AreaTop3Product> areaTop3ProductList);
}
