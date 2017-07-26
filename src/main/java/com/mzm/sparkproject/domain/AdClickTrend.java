package com.mzm.sparkproject.domain;

/**
 * 最近一小时广告点击趋势
 * Created by Administrator on 2017/7/26.
 */
public class AdClickTrend {

    //日期
    private String date;
    //小时
    private String hour;
    //分钟
    private String minute;
    //广告ID
    private long adId;
    //点击量
    private long clickCount;

    public AdClickTrend() {
    }

    public AdClickTrend(String date, String hour, String minute, long adId, long clickCount) {
        this.date = date;
        this.hour = hour;
        this.minute = minute;
        this.adId = adId;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
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
