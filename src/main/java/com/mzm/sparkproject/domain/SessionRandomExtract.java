package com.mzm.sparkproject.domain;

/**
 * 随机抽取的会话信息
 * Created by Administrator on 2017/7/21.
 */
public class SessionRandomExtract {

    //任务ID
    private long taskId;
    //会话ID
    private String sessionId;
    //会话开始时间
    private String startTime;
    //搜索关键词
    private String searchKeywords;
    //品类ID
    private String clickCategoryIds;

    public SessionRandomExtract() {
    }

    public SessionRandomExtract(long taskId, String sessionId, String startTime, String searchKeywords, String
            clickCategoryIds) {
        this.taskId = taskId;
        this.sessionId = sessionId;
        this.startTime = startTime;
        this.searchKeywords = searchKeywords;
        this.clickCategoryIds = clickCategoryIds;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getSearchKeywords() {
        return searchKeywords;
    }

    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }

    public String getClickCategoryIds() {
        return clickCategoryIds;
    }

    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }
}
