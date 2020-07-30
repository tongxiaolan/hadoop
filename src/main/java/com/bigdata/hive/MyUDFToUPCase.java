package com.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 自定义hive udf函数
 * @author 小懒
 * @create 2020/7/29
 * @since 1.0.0
 */
public class MyUDFToUPCase extends UDF{

    /**
     * 自定义函数名称必须为 evaluate 可以多个方法重载
     * @param field
     * @return
     */
    public String evaluate(String field) {
        return field.toUpperCase();
    }
}