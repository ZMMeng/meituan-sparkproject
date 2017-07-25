package com.mzm.sparkproject.domain;

/**
 * 各区域Top3热门商品
 * Created by Administrator on 2017/7/25.
 */
public class AreaTop3Product {

    //任务ID
    private long taskId;
    //区域
    private String area;
    //区域级别
    private String areaLevel;
    //产品ID
    private long productId;
    //城市名称
    private String cityInfos;
    //点击次数
    private long clickCount;
    //产品名称
    private String productName;
    //产品状态
    private String productStatus;

    public AreaTop3Product() {
    }

    public AreaTop3Product(long taskId, String area, String areaLevel, long productId, String cityInfos, long
            clickCount, String productName, String productStatus) {
        this.taskId = taskId;
        this.area = area;
        this.areaLevel = areaLevel;
        this.productId = productId;
        this.cityInfos = cityInfos;
        this.clickCount = clickCount;
        this.productName = productName;
        this.productStatus = productStatus;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getAreaLevel() {
        return areaLevel;
    }

    public void setAreaLevel(String areaLevel) {
        this.areaLevel = areaLevel;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public String getCityInfos() {
        return cityInfos;
    }

    public void setCityInfos(String cityInfos) {
        this.cityInfos = cityInfos;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductStatus() {
        return productStatus;
    }

    public void setProductStatus(String productStatus) {
        this.productStatus = productStatus;
    }
}
