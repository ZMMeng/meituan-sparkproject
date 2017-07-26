package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.IAdBlackListDao;
import com.mzm.sparkproject.domain.AdBlackList;
import com.mzm.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告黑名单用户DAO的实现类
 * Created by Administrator on 2017/7/26.
 */
public class AdBlackListDaoImpl implements IAdBlackListDao {

    /**
     * 批量向MySQL插入广告黑名单用户
     *
     * @param adBlackListList 广告黑名单用户
     */
    @Override
    public void insertBatch(List<AdBlackList> adBlackListList) {
        String sql = "INSERT INTO ad_black_list VALUES (?);";

        List<Object[]> paramsList = new ArrayList<>();

        for (AdBlackList adBlackList : adBlackListList) {
            Object[] params = {adBlackList.getUserId()};

            paramsList.add(params);
        }

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }

    /**
     * 在MySQL中查询所有广告黑名单用户
     *
     * @return 所有广告黑名单用户
     */
    @Override
    public List<AdBlackList> findAll() {
        String sql = "SELECT * FROM ad_black_list;";

        final List<AdBlackList> adBlackLists = new ArrayList<>();

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeQuery(sql, null, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    long userId = rs.getInt("user_id");

                    AdBlackList adBlackList = new AdBlackList();
                    adBlackList.setUserId(userId);

                    adBlackLists.add(adBlackList);
                }
            }
        });
        return adBlackLists;
    }
}
