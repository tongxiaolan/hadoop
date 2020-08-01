package com.bigdata.hbase.coprocesser;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 协处理器（Hbase自己的功能）
 * 1 继承BaseRegionObserver
 * 2 重写方法:
 * 3 实现逻辑
 *  向 student中增加数据的同时，向my_np:student中添加数据
 *
 *  4 将项目打包为依赖jar包，上传hbase 放到hbase/lib下  重启hbase
 *  5 重新建表student
 * @author 小懒
 * @create 2020/8/1
 * @since 1.0.0
 */
public class InsertMyNpStudentCoprocesser extends BaseRegionObserver {

    /**
     *  prePut 在put之前
     *  doPut  执行
     *  postPut 在put之后
     * @param e 当前的环境
     * @param put 插入的数据
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("my_np:student"));
        //增加数据
        table.put(put);
        // 关闭
        table.close();

    }
}