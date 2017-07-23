package com.mzm.sparkproject.domain;

/**
 * Top10品类
 * Created by 蒙卓明 on 2017/7/22.
 */
public class Top10Category {

    //任务ID
    private long taskId;
    //品类ID
    private long categoryId;
    //点击次数
    private long clickCount;
    //下单次数
    private long orderCount;
    //支付次数
    private long payCount;

    public Top10Category() {
    }

    public Top10Category(long taskId, long categoryId, long clickCount, long orderCount, long payCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
