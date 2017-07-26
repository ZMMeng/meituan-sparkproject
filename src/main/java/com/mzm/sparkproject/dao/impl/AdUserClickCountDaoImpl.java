package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IAdUserClickCountDao;
import com.mzm.sparkproject.domain.AdUserClickCount;
import com.mzm.sparkproject.jdbc.JdbcHelper;
import com.mzm.sparkproject.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户广告点击量DAO的实现类
 * Created by Administrator on 2017/7/26.
 */
public class AdUserClickCountDaoImpl implements IAdUserClickCountDao {

    /**
     * 向MySQL中插入用户广告点击量的统计结果
     *
     * @param adUserClickCountList 用户广告点击量的统计结果
     */
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCountList) {

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

        String selectSql = "SELECT COUNT(1) FROM ad_user_click_count WHERE `date`=? AND `user_id`=? AND `ad_id`=? AND" +
                " `click_count`=?;";

        Object[] selectParams = null;
        for (AdUserClickCount adUserClickCount : adUserClickCountList) {
            final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

            selectParams = new Object[]{adUserClickCount.getDate(), adUserClickCount.getUserId(),
                    adUserClickCount.getAdId(), adUserClickCount.getClickCount()};

            jdbcHelper.executeQuery(selectSql, selectParams, new JdbcHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });

            int count = queryResult.getCount();

            if (count > 0) {
                //已存在，更新即可
                updateAdUserClickCounts.add(adUserClickCount);
            } else {
                //不存在，插入即可
                insertAdUserClickCounts.add(adUserClickCount);
            }
        }

        //执行批量插入
        String insertSql = "INSERT INTO ad_user_click_count VALUES (?,?,?,?);";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
            Object[] insertParams = {adUserClickCount.getDate(), adUserClickCount.getUserId(),
                    adUserClickCount.getAdId(), adUserClickCount.getClickCount()};

            insertParamsList.add(insertParams);
        }
        jdbcHelper.executeBatch(insertSql, insertParamsList);

        //执行批量更新
        String updateSql = "UPDATE ad_user_click_count SET `click_count`=`click_count` + ? WHERE `date`=? AND `user_id`=? AND " +
                "`ad_id`=?;";
        List<Object[]> updateParamsList = new ArrayList<>();
        for(AdUserClickCount adUserClickCount : updateAdUserClickCounts){
            Object[] updateParams = {adUserClickCount.getClickCount(), adUserClickCount.getDate(),
                    adUserClickCount.getUserId(), adUserClickCount.getAdId()};

            updateParamsList.add(updateParams);
        }
        jdbcHelper.executeBatch(updateSql, updateParamsList);
    }

    /**
     * 在MySQL中根据多个key查询用户广告点击量
     *
     * @param date   日期
     * @param userId 用户ID
     * @param adId   广告ID
     * @return 用户广告点击量
     */
    @Override
    public long findClickCountByMultiKey(String date, long userId, long adId) {

        String sql = "SELECT `click_count` FROM ad_user_click_count WHERE `date`=? AND `user_id`=? AND `ad_id`=?;";

        Object[] params = {date, userId, adId};

        final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    int clickCount = rs.getInt(1);
                    queryResult.setClickCount(clickCount);
                }
            }
        });
        return queryResult.getClickCount();
    }
}
