package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ITop10CategoryDao;
import com.mzm.sparkproject.domain.Top10Category;
import com.mzm.sparkproject.jdbc.JdbcHelper;

/**
 * Top10热门品类的DAO实现类
 * Created by 蒙卓明 on 2017/7/22.
 */
public class Top10CategoryDaoImpl implements ITop10CategoryDao {

    /**
     * 向MySQL中插入Top10品类的统计结果
     *
     * @param top10Category Top10品类
     */
    @Override
    public void insert(Top10Category top10Category) {

        String sql = "insert into top10_category values (?,?,?,?,?);";

        Object[] params = {top10Category.getTaskId(),
                top10Category.getCategoryId(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
