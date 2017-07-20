package com.mzm.sparkproject.spark;

import com.mzm.sparkproject.constants.Constants;
import com.mzm.sparkproject.utils.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * session聚合统计Accumulator
 * Created by Administrator on 2017/7/20.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    /**
     * 数据初始化
     *
     * @param initialValue 初始化值
     * @return 所有范围区间的数量，即0
     */
    @Override
    public String zero(String initialValue) {
        StringBuilder sb = new StringBuilder();
        sb.append(Constants.SESSION_COUNT).append("=0|")
                .append(Constants.TIME_PERIOD_1s_3s).append("=0|")
                .append(Constants.TIME_PERIOD_4s_6s).append("=0|")
                .append(Constants.TIME_PERIOD_7s_9s).append("=0|")
                .append(Constants.TIME_PERIOD_10s_30s).append("=0|")
                .append(Constants.TIME_PERIOD_30s_60s).append("=0|")
                .append(Constants.TIME_PERIOD_1m_3m).append("=0|")
                .append(Constants.TIME_PERIOD_3m_10m).append("=0|")
                .append(Constants.TIME_PERIOD_10m_30m).append("=0|")
                .append(Constants.TIME_PERIOD_30m).append("=0|")
                .append(Constants.STEP_PERIOD_1_3).append("=0|")
                .append(Constants.STEP_PERIOD_4_6).append("=0|")
                .append(Constants.STEP_PERIOD_7_9).append("=0|")
                .append(Constants.STEP_PERIOD_10_30).append("=0|")
                .append(Constants.STEP_PERIOD_30_60).append("=0|")
                .append(Constants.STEP_PERIOD_60).append("=0");
        return sb.toString();
    }

    /**
     * 统计计算逻辑
     *
     * @param v1 连接串
     * @param v2 范围区间字符串
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {

        if (StringUtils.isEmpty(v1)) {
            //v1为空，直接返回v2
            return v2;
        }

        //从v1中获取v2对应的值
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            //将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            //使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }

        //v1中没有v2对应的值，直接返回v1
        return v1;
    }
}
