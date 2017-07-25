package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IAreaTop3ProductDao;
import com.mzm.sparkproject.domain.AreaTop3Product;
import com.mzm.sparkproject.jdbc.JdbcHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 各区域Top3热门商品DAO接口的实现类
 * Created by Administrator on 2017/7/25.
 */
public class AreaTop3ProductListDaoImpl implements IAreaTop3ProductDao {

    /**
     * 将各区域Top3热门商品批量插入MySQL中
     *
     * @param areaTop3ProductList 各区域Top3热门商品
     */
    @Override
    public void insertBatch(List<AreaTop3Product> areaTop3ProductList) {

        String sql = "INSERT INTO area_top3_product VALUES (?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList = new ArrayList<>();

        for(AreaTop3Product areaTop3Product : areaTop3ProductList){
            Object[] params = new Object[]{areaTop3Product.getTaskId(),
                    areaTop3Product.getArea(),
                    areaTop3Product.getAreaLevel(),
                    areaTop3Product.getProductId(),
                    areaTop3Product.getCityInfos(),
                    areaTop3Product.getClickCount(),
                    areaTop3Product.getProductName(),
                    areaTop3Product.getProductStatus()
            };
            paramsList.add(params);
        }

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }
}
