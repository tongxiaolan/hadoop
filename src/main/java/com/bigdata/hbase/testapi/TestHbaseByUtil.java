package com.bigdata.hbase.testapi;

import com.bigdata.hbase.utils.HbaseUtil;

import java.io.IOException;

/**
 * 测试自己编写的工具类  HbaseUtil
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class TestHbaseByUtil {

    public static void main(String[] args) throws IOException {
        //创建连接
        HbaseUtil.mkaeHbaseConnection();

        HbaseUtil.insertData("my_np:student","1002","info","name","lisi");

        // 关闭连接
        HbaseUtil.closeHbaseConnection();
    }
}