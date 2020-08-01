package com.bigdata.hbase.mr;

import com.bigdata.hbase.mr.tool.FileToTableTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author 小懒
 * @create 2020/8/1
 * @since 1.0.0
 */
public class FileToTable {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FileToTableTool(), args);
    }
}