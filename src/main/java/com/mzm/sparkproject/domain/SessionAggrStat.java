package com.mzm.sparkproject.domain;

/**
 * 会话聚合统计结果类
 * Created by Administrator on 2017/7/20.
 */
public class SessionAggrStat {

    //任务ID
    private long taskId;
    //会话总数
    private long sessionCount;
    //访问时长在1s~3s的会话比例
    private double timePeriod1sTo3sRatio;
    //访问时长在4s~6s的会话比例
    private double timePeriod4sTo6sRatio;
    //访问时长在7s~9s的会话比例
    private double timePeriod7sTo9sRatio;
    //访问时长在10s~30s的会话比例
    private double timePeriod10sTo30sRatio;
    //访问时长在30s~60s的会话比例
    private double timePeriod30sTo60sRatio;
    //访问时长在1m~3m的会话比例
    private double timePeriod1mTo3mRatio;
    //访问时长在3m~10m的会话比例
    private double timePeriod3mTo10mRatio;
    //访问时长在10m~30m的会话比例
    private double timePeriod10mTo30mRatio;
    //访问时长在30m以上的会话比例
    private double timePeriod30mRatio;
    //访问步长在1~3之间的会话比例
    private double stepLength1To3Ratio;
    //访问步长在4~6之间的会话比例
    private double stepLength4To6Ratio;
    //访问步长在7~9之间的会话比例
    private double stepLength7To9Ratio;
    //访问步长在10~30之间的会话比例
    private double stepLength10To30Ratio;
    //访问步长在30~60之间的会话比例
    private double stepLength30To60Ratio;
    //访问步长在60以上的会话比例
    private double stepLength60Ratio;

    public SessionAggrStat() {
    }

    public SessionAggrStat(long taskId, long sessionCount, double timePeriod1sTo3sRatio, double
            timePeriod4sTo6sRatio, double timePeriod7sTo9sRatio, double timePeriod10sTo30sRatio, double
            timePeriod30sTo60sRatio, double timePeriod1mTo3mRatio, double timePeriod3mTo10mRatio, double
            timePeriod10mTo30mRatio, double timePeriod30mRatio, double stepLength1To3Ratio, double
            stepLength4To6Ratio, double stepLength7To9Ratio, double stepLength10To30Ratio, double
            stepLength30To60Ratio, double stepLength60Ratio) {
        this.taskId = taskId;
        this.sessionCount = sessionCount;
        this.timePeriod1sTo3sRatio = timePeriod1sTo3sRatio;
        this.timePeriod4sTo6sRatio = timePeriod4sTo6sRatio;
        this.timePeriod7sTo9sRatio = timePeriod7sTo9sRatio;
        this.timePeriod10sTo30sRatio = timePeriod10sTo30sRatio;
        this.timePeriod30sTo60sRatio = timePeriod30sTo60sRatio;
        this.timePeriod1mTo3mRatio = timePeriod1mTo3mRatio;
        this.timePeriod3mTo10mRatio = timePeriod3mTo10mRatio;
        this.timePeriod10mTo30mRatio = timePeriod10mTo30mRatio;
        this.timePeriod30mRatio = timePeriod30mRatio;
        this.stepLength1To3Ratio = stepLength1To3Ratio;
        this.stepLength4To6Ratio = stepLength4To6Ratio;
        this.stepLength7To9Ratio = stepLength7To9Ratio;
        this.stepLength10To30Ratio = stepLength10To30Ratio;
        this.stepLength30To60Ratio = stepLength30To60Ratio;
        this.stepLength60Ratio = stepLength60Ratio;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(long sessionCount) {
        this.sessionCount = sessionCount;
    }

    public double getTimePeriod1sTo3sRatio() {
        return timePeriod1sTo3sRatio;
    }

    public void setTimePeriod1sTo3sRatio(double timePeriod1sTo3sRatio) {
        this.timePeriod1sTo3sRatio = timePeriod1sTo3sRatio;
    }

    public double getTimePeriod4sTo6sRatio() {
        return timePeriod4sTo6sRatio;
    }

    public void setTimePeriod4sTo6sRatio(double timePeriod4sTo6sRatio) {
        this.timePeriod4sTo6sRatio = timePeriod4sTo6sRatio;
    }

    public double getTimePeriod7sTo9sRatio() {
        return timePeriod7sTo9sRatio;
    }

    public void setTimePeriod7sTo9sRatio(double timePeriod7sTo9sRatio) {
        this.timePeriod7sTo9sRatio = timePeriod7sTo9sRatio;
    }

    public double getTimePeriod10sTo30sRatio() {
        return timePeriod10sTo30sRatio;
    }

    public void setTimePeriod10sTo30sRatio(double timePeriod10sTo30sRatio) {
        this.timePeriod10sTo30sRatio = timePeriod10sTo30sRatio;
    }

    public double getTimePeriod30sTo60sRatio() {
        return timePeriod30sTo60sRatio;
    }

    public void setTimePeriod30sTo60sRatio(double timePeriod30sTo60sRatio) {
        this.timePeriod30sTo60sRatio = timePeriod30sTo60sRatio;
    }

    public double getTimePeriod1mTo3mRatio() {
        return timePeriod1mTo3mRatio;
    }

    public void setTimePeriod1mTo3mRatio(double timePeriod1mTo3mRatio) {
        this.timePeriod1mTo3mRatio = timePeriod1mTo3mRatio;
    }

    public double getTimePeriod3mTo10mRatio() {
        return timePeriod3mTo10mRatio;
    }

    public void setTimePeriod3mTo10mRatio(double timePeriod3mTo10mRatio) {
        this.timePeriod3mTo10mRatio = timePeriod3mTo10mRatio;
    }

    public double getTimePeriod10mTo30mRatio() {
        return timePeriod10mTo30mRatio;
    }

    public void setTimePeriod10mTo30mRatio(double timePeriod10mTo30mRatio) {
        this.timePeriod10mTo30mRatio = timePeriod10mTo30mRatio;
    }

    public double getTimePeriod30mRatio() {
        return timePeriod30mRatio;
    }

    public void setTimePeriod30mRatio(double timePeriod30mRatio) {
        this.timePeriod30mRatio = timePeriod30mRatio;
    }

    public double getStepLength1To3Ratio() {
        return stepLength1To3Ratio;
    }

    public void setStepLength1To3Ratio(double stepLength1To3Ratio) {
        this.stepLength1To3Ratio = stepLength1To3Ratio;
    }

    public double getStepLength4To6Ratio() {
        return stepLength4To6Ratio;
    }

    public void setStepLength4To6Ratio(double stepLength4To6Ratio) {
        this.stepLength4To6Ratio = stepLength4To6Ratio;
    }

    public double getStepLength7To9Ratio() {
        return stepLength7To9Ratio;
    }

    public void setStepLength7To9Ratio(double stepLength7To9Ratio) {
        this.stepLength7To9Ratio = stepLength7To9Ratio;
    }

    public double getStepLength10To30Ratio() {
        return stepLength10To30Ratio;
    }

    public void setStepLength10To30Ratio(double stepLength10To30Ratio) {
        this.stepLength10To30Ratio = stepLength10To30Ratio;
    }

    public double getStepLength30To60Ratio() {
        return stepLength30To60Ratio;
    }

    public void setStepLength30To60Ratio(double stepLength30To60Ratio) {
        this.stepLength30To60Ratio = stepLength30To60Ratio;
    }

    public double getStepLength60Ratio() {
        return stepLength60Ratio;
    }

    public void setStepLength60Ratio(double stepLength60Ratio) {
        this.stepLength60Ratio = stepLength60Ratio;
    }
}