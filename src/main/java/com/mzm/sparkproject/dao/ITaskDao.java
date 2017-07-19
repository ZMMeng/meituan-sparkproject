package com.mzm.sparkproject.dao;

import com.mzm.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 * Created by Administrator on 2017/7/19.
 */
public interface ITaskDao {

    /**
     * 根据主键查询任务
     *
     * @param taskId 任务ID
     * @return 任务对象
     */
    Task findById(long taskId);
}
