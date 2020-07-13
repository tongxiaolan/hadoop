package com.bigdata.mr.join.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO MAP join
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class MJMapper extends Mapper<LongWritable,Text,Text,NullWritable>{


    /**
     * 用来放缓存文件
     */
    private Map<String, String> pMap = new HashMap<>();

    private Text k = new Text();

    /**
     * 将pd文件读取到map中
     *   01	小米
         02	华为
         03	格力

     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFils = context.getCacheFiles();
        String path = cacheFils[0].getPath().toString();
        // 本地流
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] fields = line.split("\t");
            pMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    /**
     *
     1001	01	1
     1002	02	2
     1003	03	3
     1004	01	4
     1005	02	5
     1006	03	6
     -->
     1001	小米	1
     1002	华为	2
     1003	格力	3
     1004	小米	4
     1005	华为	5
     1006	格力	6
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String pname = pMap.get(fields[1]);
        k.set(fields[0] + "\t" + pname + "\t" + fields[2]);
        context.write(k,NullWritable.get());
    }
}