package com.mzm.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * Created by Administrator on 2017/7/25.
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {

    @Override
    public String call(String json, String field) throws Exception {

        try {
            JSONObject jsonObj = JSONObject.parseObject(json);
            return jsonObj.getString(field);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
