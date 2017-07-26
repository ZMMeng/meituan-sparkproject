package com.mzm.sparkproject.model;

/**
 * 用户广告点击量查询结果
 * Created by Administrator on 2017/7/26.
 */
public class AdUserClickCountQueryResult {

    //插入数量
    private int count;
    //广告点击量
    private long clickCount;

    public AdUserClickCountQueryResult() {
    }

    public AdUserClickCountQueryResult(int count, long clickCount) {
        this.count = count;
        this.clickCount = clickCount;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
