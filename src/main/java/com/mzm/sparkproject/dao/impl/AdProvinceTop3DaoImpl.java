package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IAdProvinceTop3Dao;
import com.mzm.sparkproject.domain.AdProvinceTop3;
import com.mzm.sparkproject.jdbc.JdbcHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 每天各省Top3热门广告DAO实现类
 * Created by Administrator on 2017/7/26.
 */
public class AdProvinceTop3DaoImpl implements IAdProvinceTop3Dao {

    /**
     * 批量更新每天各省Top3热门广告的统计结果
     *
     * @param adProvinceTop3List 每天各省Top3热门广告
     */
    @Override
    public void updateBatch(List<AdProvinceTop3> adProvinceTop3List) {

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        //先去重
        List<String> dateProvinceList = new ArrayList<>();
        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3List) {
            String date = adProvinceTop3.getDate();
            String province = adProvinceTop3.getProvince();

            String key = date + "_" + province;

            if (!dateProvinceList.contains(key)) {
                dateProvinceList.add(key);
            }
        }

        //根据去重后的日期和省份进行批量删除
        String deleteSql = "DELETE FROM ad_province_top3 WHERE `date`=? AND `province`=?;";
        List<Object[]> deleteParamsList = new ArrayList<>();
        for (String dateProvice : dateProvinceList) {
            String date = dateProvice.split("_")[0];
            String province = dateProvice.split("_")[1];

            Object[] params = {date, province};
            deleteParamsList.add(params);
        }
        jdbcHelper.executeBatch(deleteSql, deleteParamsList);

        //批量插入传入的所有数据
        String insertSql = "INSERT INTO ad_province_top3 VALUES (?,?,?,?);";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3List) {
            Object[] params = {adProvinceTop3.getDate(), adProvinceTop3.getProvince(), adProvinceTop3.getAdId(),
                    adProvinceTop3.getClickCount()};

            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSql, insertParamsList);
    }
}
