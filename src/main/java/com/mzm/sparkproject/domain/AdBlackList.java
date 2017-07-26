package com.mzm.sparkproject.domain;

/**
 * 广告黑名单用户
 * Created by Administrator on 2017/7/26.
 */
public class AdBlackList {

    //用户ID
    private long userId;

    public AdBlackList() {
    }

    public AdBlackList(long userId) {
        this.userId = userId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }
}
