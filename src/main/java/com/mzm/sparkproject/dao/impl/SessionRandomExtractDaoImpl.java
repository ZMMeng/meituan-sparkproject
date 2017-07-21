package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ISessionRandomExtractDao;
import com.mzm.sparkproject.domain.SessionRandomExtract;
import com.mzm.sparkproject.jdbc.JdbcHelper;

/**
 * session随机抽取模块的DAO实现类
 * Created by Administrator on 2017/7/21.
 */
public class SessionRandomExtractDaoImpl implements ISessionRandomExtractDao {

    /**
     * 向MySQL中插入随机抽取的session信息
     *
     * @param sessionRandomExtract
     */
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {

        String sql = "insert into session_random_extract values (?,?,?,?,?);";

        Object[] params = {sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
