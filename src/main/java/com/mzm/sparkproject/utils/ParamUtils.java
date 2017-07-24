package com.mzm.sparkproject.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mzm.sparkproject.conf.ConfigurationManager;
import com.mzm.sparkproject.constants.Constants;

/**
 * 参数工具类
 * Created by Administrator on 2017/7/19.
 */
public class ParamUtils {

    /**
     * 从命令行参数中提取任务id
     *
     * @param args 命令行参数
     * @param type 任务类型
     * @return 任务id
     */
    public static Long getTaskIdFromArgs(String[] args, String type) {

        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(local){
            return ConfigurationManager.getLong(type);
        }

        try {
            if (args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException("从命令行获取参数失败", e);
        }
    }

    /**
     * 从JSON对象中提取参数
     *
     * @param jsonObject JSON对象
     * @return 参数
     */
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if (jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }
}
