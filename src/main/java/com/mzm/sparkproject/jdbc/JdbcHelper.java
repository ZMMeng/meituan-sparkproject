package com.mzm.sparkproject.jdbc;

import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC辅助组件
 * Created by Administrator on 2017/7/19.
 */
public class JdbcHelper {

    //单例模式
    private static JdbcHelper instance = null;
    // 数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    //加载数据库驱动
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError("数据库驱动加载失败！");
        }
    }

    /**
     * 私有化构造方法，同时创建唯一的数据库连接池
     */
    private JdbcHelper() {
        //获取数据库连接池大小、URL、用户名和密码
        int dataSourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String userName = ConfigurationManager.getProperty(Constants.JDBC_USERNAME);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

        //创建指定数量的数据库连接，并放入连接池中
        for (int i = 0; i < dataSourceSize; i++) {
            try {
                Connection conn = DriverManager.getConnection(url, userName, password);
                datasource.push(conn);
            } catch (SQLException e) {
                throw new RuntimeException("获取数据库连接失败！", e);
            }
        }
    }

    /**
     * 获取单例
     *
     * @return 单例对象
     */
    public static JdbcHelper getInstance() {
        if (instance == null) {
            synchronized (JdbcHelper.class) {
                if (instance == null) {
                    instance = new JdbcHelper();
                }
            }
        }
        return instance;
    }

    /**
     * 提供获取数据库连接的方法
     *
     * @return 数据库连接
     */
    public synchronized Connection getConnection() {
        //暂时获取不到连接，先让线程睡眠
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return datasource.poll();
    }

    /**
     * 执行增删改操作
     *
     * @param sql    SQL语句
     * @param params SQL语句的参数
     * @return 增删改影响行数
     */
    public int executeUpdate(String sql, Object[] params) {
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("执行数据库增删改操作失败！", e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    //nothing
                }
            }

            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 执行查询操作
     *
     * @param sql      SQL语句
     * @param params   SQL语句中的参数
     * @param callBack 回调方法
     */
    public void executeQuery(String sql, Object[] params, QueryCallBack callBack) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        conn = getConnection();
        try {
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rs = pstmt.executeQuery();
            callBack.process(rs);
        } catch (Exception e) {
            throw new RuntimeException("执行数据库查询操作失败！", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    //nothing
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    //nothing
                }
            }

            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行SQL语句
     *
     * @param sql       SQL语句
     * @param paramList SQL语句的参数
     * @return 执行后影响的行数
     */
    public int[] executeBatch(String sql, List<Object[]> paramList) {
        Connection conn = null;
        PreparedStatement pstmt = null;

        conn = getConnection();
        try {
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            for(Object[] params : paramList){
                for(int i = 0; i < params.length; i++){
                    pstmt.setObject(i + 1, params[i]);
                }
                pstmt.addBatch();
            }
            int[] rtn = pstmt.executeBatch();
            conn.commit();
            return rtn;
        } catch (SQLException e) {
            throw new RuntimeException("执行数据库批量操作失败！", e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    //nothing
                }
            }

            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 内部接口：查询回调接口
     */
    public interface QueryCallBack {

        /**
         * 处理查询结果
         *
         * @param rs 结果集
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
