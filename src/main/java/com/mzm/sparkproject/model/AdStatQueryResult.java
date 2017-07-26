package com.mzm.sparkproject.model;

/**
 * 每天各省各城市各支广告点击量的查询结果
 * Created by Administrator on 2017/7/26.
 */
public class AdStatQueryResult {

    //表征该天该省该城市该支广告的点击量是否已在MySQL表中
    private int count;

    public AdStatQueryResult() {
    }

    public AdStatQueryResult(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
