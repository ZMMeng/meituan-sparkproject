package com.mzm.sparkproject.dao.impl;

import com.mzm.sparkproject.dao.ITaskDao;
import com.mzm.sparkproject.domain.Task;
import com.mzm.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;

/**
 * 任务管理DAO实现类
 * Created by Administrator on 2017/7/19.
 */
public class TaskDaoImpl implements ITaskDao {

    /**
     * 根据主键查询任务
     *
     * @param taskId 任务ID
     * @return 任务对象
     */
    @Override
    public Task findById(long taskId) {
        String sql = "select * from task where task_id=?;";
        Object[] params = {taskId};
        Task task = new Task();

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    task.setTaskId(rs.getLong("task_id"));
                    task.setTaskName(rs.getString("task_name"));
                    task.setCreateTime(rs.getString("create_time"));
                    task.setStartTime(rs.getString("start_time"));
                    task.setFinishTime(rs.getString("finish_time"));
                    task.setTaskType(rs.getString("task_type"));
                    task.setTaskStatus(rs.getString("task_status"));
                    task.setTaskParam(rs.getString("task_param"));
                }
            }
        });
        return task;
    }
}
