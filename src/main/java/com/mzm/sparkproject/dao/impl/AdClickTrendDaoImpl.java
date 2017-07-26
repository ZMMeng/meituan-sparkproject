package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IAdClickTrendDao;
import com.mzm.sparkproject.domain.AdClickTrend;
import com.mzm.sparkproject.jdbc.JdbcHelper;
import com.mzm.sparkproject.model.AdClickTrendQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 最近一小时广告点击趋势DAO实现类
 * Created by Administrator on 2017/7/26.
 */
public class AdClickTrendDaoImpl implements IAdClickTrendDao {

    /**
     * 向MySQL中更新最近一小时广告点击趋势
     *
     * @param adClickTrendList 最近一小时广告点击趋势
     */
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrendList) {

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        //区分数据的插入和更新
        List<AdClickTrend> insertAdClickTrendList = new ArrayList<>();
        List<AdClickTrend> updateAdClickTrendList = new ArrayList<>();

        String selectSql = "SELECT COUNT(1) FROM ad_click_trend WHERE `date`=? AND `hour`=? AND `minute`=? AND " +
                "`ad_id`=?;";
        final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();
        for (AdClickTrend adClickTrend : adClickTrendList) {
            Object[] params = {adClickTrend.getDate(), adClickTrend.getHour(), adClickTrend.getMinute(),
                    adClickTrend.getAdId()};

            jdbcHelper.executeQuery(selectSql, params, new JdbcHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        queryResult.setCount(rs.getInt(1));
                    }
                }
            });

            if (queryResult.getCount() > 0) {
                updateAdClickTrendList.add(adClickTrend);
            } else {
                insertAdClickTrendList.add(adClickTrend);
            }
        }

        //批量插入
        String insertSql = "INSERT INTO ad_click_trend VALUES (?,?,?,?,?);";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdClickTrend adClickTrend : insertAdClickTrendList) {
            Object[] params = {adClickTrend.getDate(), adClickTrend.getHour(), adClickTrend.getMinute(),
                    adClickTrend.getAdId(), adClickTrend.getClickCount()};

            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSql, insertParamsList);

        //批量更新
        String updateSql = "UPDATE ad_click_trend SET `click_count`=? WHERE `date`=? AND `hour`=? AND `minute`=? AND " +
                "`ad_id`=?;";
        List<Object[]> updateParamsList = new ArrayList<>();
        for (AdClickTrend adClickTrend : updateAdClickTrendList) {
            Object[] params = {adClickTrend.getClickCount(), adClickTrend.getDate(), adClickTrend.getHour(),
                    adClickTrend.getMinute(), adClickTrend.getAdId()};

            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSql, updateParamsList);

    }
}
