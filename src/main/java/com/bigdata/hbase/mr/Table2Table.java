package com.bigdata.hbase.mr;

import com.bigdata.hbase.mr.tool.HbaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 表数据导入表数据mr入口
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class Table2Table {
    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new HbaseMapperReduceTool(), args);
    }
}