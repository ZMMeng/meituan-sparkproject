package com.mzm.sparkproject.domain;

/**
 * 每天各省Top3热门广告
 * Created by Administrator on 2017/7/26.
 */
public class AdProvinceTop3 {

    //日期
    private String date;
    //省份
    private String province;
    //广告ID
    private long adId;
    //点击量
    private long clickCount;

    public AdProvinceTop3() {
    }

    public AdProvinceTop3(String date, String province, long adId, long clickCount) {
        this.date = date;
        this.province = province;
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
