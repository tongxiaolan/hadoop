/**
 * Copyright (C), 2015-2020, 中发展信有限公司
 * FileName: WordCountReduce
 * Author:   小懒
 * Date:     2020/7/9 18:21
 * Description:
 * History:
 * <author>           <version>          <desc>
 * 作者姓名            版本号              描述
 */
package com.bigdata.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{


    private IntWritable sumNumber = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        sumNumber.set(sum);
        context.write(key,sumNumber);
    }
}