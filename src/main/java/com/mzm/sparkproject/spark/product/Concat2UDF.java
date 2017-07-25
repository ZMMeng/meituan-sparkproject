package com.mzm.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * 将两个字段拼接起来，可使用指定的分隔符
 * Created by Administrator on 2017/7/25.
 */
public class Concat2UDF implements UDF3<String, String, String, String>{

    @Override
    public String call(String v1, String v2, String split) throws Exception {
        return v1 + split + v2;
    }
}
