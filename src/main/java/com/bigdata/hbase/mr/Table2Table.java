package com.bigdata.hbase.mr;

import com.bigdata.hbase.mr.tool.HbaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class Table2Table {
    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new HbaseMapperReduceTool(), args);
    }
}