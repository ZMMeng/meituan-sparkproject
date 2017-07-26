package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IAdStatDao;
import com.mzm.sparkproject.domain.AdStat;
import com.mzm.sparkproject.jdbc.JdbcHelper;
import com.mzm.sparkproject.model.AdStatQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 每天各省各城市各支广告点击量的实时统计DAO实现类
 * Created by Administrator on 2017/7/26.
 */
public class AdStatDaoImpl implements IAdStatDao {

    /**
     * 向MySQL中插入每天各省各城市各支广告点击量的实时统计
     *
     * @param adStatList 每天各省各城市各支广告点击量的实时统计
     */
    @Override
    public void updateBatch(List<AdStat> adStatList) {

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        //区分插入和更新
        List<AdStat> insertAdStatList = new ArrayList<>();
        List<AdStat> updateAdStatList = new ArrayList<>();

        //查询结果
        final AdStatQueryResult queryResult = new AdStatQueryResult();

        //查询数据库，确定该条记录是插入还是更新
        String selectSql = "SELECT COUNT(1) FROM ad_stat WHERE `date`=? AND `province`=? AND `city`=? AND `ad_id`=?;";
        for(AdStat adStat : adStatList){
            Object[] params = {adStat.getDate(), adStat.getProvince(), adStat.getCity(), adStat.getAdId()};

            jdbcHelper.executeQuery(selectSql, params, new JdbcHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()){
                        queryResult.setCount(rs.getInt(1));
                    }
                }
            });

            if(queryResult.getCount() > 0){
                updateAdStatList.add(adStat);
            }else{
                insertAdStatList.add(adStat);
            }
        }

        //批量插入
        String insertSql = "INSERT INTO ad_stat VALUES (?,?,?,?,?);";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdStat adStat : insertAdStatList) {
            Object[] params = {adStat.getDate(), adStat.getProvince(), adStat.getCity(), adStat.getAdId(),
                    adStat.getClickCount()};

            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSql, insertParamsList);

        //批量更新
        String updateSql = "UPDATE ad_stat SET `click_count`=? WHERE `date`=? AND `province`=? AND `city`=? AND `ad_id`=?;";
        List<Object[]> updateParamsList = new ArrayList<>();
        for(AdStat adStat : updateAdStatList){
            Object[] params = {adStat.getClickCount(), adStat.getDate(), adStat.getProvince(), adStat.getCity(),
                    adStat.getAdId()};

            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSql, updateParamsList);
    }
}
