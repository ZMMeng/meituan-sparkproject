package com.mzm.sparkproject.domain;

/**
 * 用户广告点击量
 * Created by Administrator on 2017/7/26.
 */
public class AdUserClickCount {

    //日期
    private String date;
    //用户ID
    private long userId;
    //广告ID
    private long adId;
    //点击量
    private long clickCount;

    public AdUserClickCount() {
    }

    public AdUserClickCount(String date, long userId, long adId, long clickCount) {
        this.date = date;
        this.userId = userId;
        this.adId = adId;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getAdId() {
        return adId;
    }

    public void setAdId(long adId) {
        this.adId = adId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
