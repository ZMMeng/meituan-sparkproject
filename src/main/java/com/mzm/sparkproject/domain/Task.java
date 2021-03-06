package com.mzm.sparkproject.domain;

import java.io.Serializable;

/**
 * 任务类
 * Created by Administrator on 2017/7/19.
 */
public class Task implements Serializable{

    //任务ID
    private long taskId;
    //任务名称
    private String taskName;
    //任务创建时间
    private String createTime;
    //任务开始时间
    private String startTime;
    //任务结束时间
    private String finishTime;
    //任务类型
    private String taskType;
    //任务状态
    private String taskStatus;
    //任务参数
    private String taskParam;

    public Task() {
    }

    public Task(long taskId, String taskName, String createTime, String startTime, String finishTime,
                String taskType, String taskStatus, String taskParam) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.createTime = createTime;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.taskType = taskType;
        this.taskStatus = taskStatus;
        this.taskParam = taskParam;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getTaskParam() {
        return taskParam;
    }

    public void setTaskParam(String taskParam) {
        this.taskParam = taskParam;
    }
}
