package com.mzm.sparkproject.domain;

/**
 * 页面切片转化率的统计结果
 * Created by 蒙卓明 on 2017/7/24.
 */
public class PageSplitConvertRate {

    //任务ID
    private long taskId;
    //页面切片转化率
    private String convertRate;

    public PageSplitConvertRate() {
    }

    public PageSplitConvertRate(long taskId, String convertRate) {
        this.taskId = taskId;
        this.convertRate = convertRate;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}
