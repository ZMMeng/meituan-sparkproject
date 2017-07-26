package com.mzm.sparkproject.domain;

/**
 * 每天各省各城市各支广告点击量的实时统计
 * Created by Administrator on 2017/7/26.
 */
public class AdStat {

    //日期
    private String date;
    //省份
    private String province;
    //城市
    private String city;
    //广告ID
    private long adId;
    //点击量
    private long clickCount;

    public AdStat() {
    }

    public AdStat(String date, String province, String city, long adId, long clickCount) {
        this.date = date;
        this.province = province;
        this.city = city;
        this.adId = adId;
        this.clickCount = clickCount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
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
