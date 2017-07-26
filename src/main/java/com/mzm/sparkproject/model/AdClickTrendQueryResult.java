package com.mzm.sparkproject.model;

/**
 * 最近一小时广告点击趋势查询结果
 * Created by Administrator on 2017/7/26.
 */
public class AdClickTrendQueryResult {

    //用于区分插入还是更新
    private int count;

    public AdClickTrendQueryResult() {
    }

    public AdClickTrendQueryResult(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
