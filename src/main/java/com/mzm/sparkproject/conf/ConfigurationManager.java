package com.mzm.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * Created by Administrator on 2017/7/19.
 */
public class ConfigurationManager {

    //Properties对象
    private static Properties prop = new Properties();

    static {
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            throw new ExceptionInInitializerError("读取配置文件失败！");
        }
    }

    /**
     * 读取指定属性的值
     *
     * @param key 属性名
     * @return 相应的属性值，以字符串形式返回
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 读取指定属性值
     *
     * @param key 属性名
     * @return 相应的属性值，以整数形式返回
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            throw new RuntimeException("读取" + key + "属性失败！", e);
        }
    }

    /**
     * 读取指定属性值
     *
     * @param key 属性名
     * @return 相应的属性值，以boolean形式返回
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            throw new RuntimeException("读取" + key + "属性失败！", e);
        }
    }
}
