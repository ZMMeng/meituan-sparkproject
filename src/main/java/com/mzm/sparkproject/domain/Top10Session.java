package com.mzm.sparkproject.domain;

/**
 * top10活跃session
 * Created by 蒙卓明 on 2017/7/22.
 */
public class Top10Session {

    //任务ID
    private long taskId;
    //品类ID
    private long categoryId;
    //会话ID
    private String sessionId;
    //点击次数
    private long clickCount;

    public Top10Session() {
    }

    public Top10Session(long taskId, long categoryId, String sessionId, long clickCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.sessionId = sessionId;
        this.clickCount = clickCount;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
