/**
 * Copyright (C), 2015-2020, 中发展信有限公司
 * FileName: WordCountMapper
 * Author:   小懒
 * Date:     2020/7/9 18:21
 * Description:
 * History:
 * <author>           <version>          <desc>
 * 作者姓名            版本号              描述
 */
package com.bigdata.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    private static final IntWritable one = new IntWritable(1);
    private Text wordT = new Text();
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] words = line.toString().split(" ");
        for (String word : words) {
            wordT.set(word);
            context.write(wordT,one);
        }
    }
}