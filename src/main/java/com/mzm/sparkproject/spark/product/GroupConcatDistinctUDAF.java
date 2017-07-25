package com.mzm.sparkproject.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数
 * Created by Administrator on 2017/7/25.
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

    //指定输入数据的字段和类型
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));

    //指定缓冲数据的字段和类型
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));

    //指定返回类型
    private DataType dataType = DataTypes.StringType;

    //指定是否是确定性的
    private boolean deterministic = true;

    /**
     * 初始化，可以在内部指定初始值
     *
     * @param buffer 缓冲数据
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    /**
     * 更新，将组内的字段值逐个传入，注意这是在单个节点上发生的
     *
     * @param buffer 缓冲数据
     * @param input  输入数据
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        //缓冲中已拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);

        //刚传入的某个城市信息
        String inputCityInfo = input.getString(0);

        //去重判断
        if (!bufferCityInfo.contains(inputCityInfo)) {
            if ("".equals(bufferCityInfo)) {
                bufferCityInfo += inputCityInfo;
            } else {
                bufferCityInfo += ("," + inputCityInfo);
            }
            //更新缓存
            buffer.update(0, bufferCityInfo);
        }
    }

    /**
     * 合并，将多个节点上同属于一个分组的数据进行合并
     *
     * @param buffer1 某个节点上的缓冲数据
     * @param buffer2 另外一个节点上的缓冲数据
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for (String cityInfo : bufferCityInfo2.split(",")) {
            if (!bufferCityInfo1.contains(cityInfo)) {
                if ("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                } else {
                    bufferCityInfo1 += ("," + cityInfo);
                }
            }
        }

        buffer1.update(0, bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row row) {
        return row.getString(0);
    }

    @Override
    public StructType inputSchema() {
        return null;
    }

    @Override
    public StructType bufferSchema() {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    @Override
    public boolean deterministic() {
        return false;
    }
}
