package com.mzm.sparkproject.domain;

/**
 * 抽取会话的详细信息
 * Created by Administrator on 2017/7/21.
 */
public class SessionDetail {

    //任务ID
    private long taskId;
    //用户ID
    private long userId;
    //会话ID
    private String sessionId;
    //页面ID
    private long pageId;
    //动作发生时间
    private String actionTime;
    //搜索关键词
    private String searchKeyword;
    //点击品类ID
    private long clickCategoryId;
    //点击产品ID
    private long clickProductId;
    //订单品类ID
    private String orderCategoryIds;
    //订单产品ID
    private String orderProductIds;
    //支付品类ID
    private String payCategoryIds;
    //支付产品ID
    private String payProductIds;

    public SessionDetail() {
    }

    public SessionDetail(long taskId, long userId, String sessionId, long pageId, String actionTime, String
            searchKeyword, long clickCategoryId, long clickProductId, String orderCategoryIds, String
            orderProductIds, String payCategoryIds, String payProductIds) {
        this.taskId = taskId;
        this.userId = userId;
        this.sessionId = sessionId;
        this.pageId = pageId;
        this.actionTime = actionTime;
        this.searchKeyword = searchKeyword;
        this.clickCategoryId = clickCategoryId;
        this.clickProductId = clickProductId;
        this.orderCategoryIds = orderCategoryIds;
        this.orderProductIds = orderProductIds;
        this.payCategoryIds = payCategoryIds;
        this.payProductIds = payProductIds;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }
}
