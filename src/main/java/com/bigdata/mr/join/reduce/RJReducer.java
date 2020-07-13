package com.bigdata.mr.join.reduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class RJReducer extends Reducer<Orderbean,NullWritable,Orderbean,NullWritable> {

    /**
     * 1 获取迭代器
     * 2 先让指针走一个(iterator.next()) 获取到第一个对象
     * 3 取出pname
     * 4 给下边的每个key(Orderbean) 设置pname
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Orderbean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()) {
            // 使指针指向下一组k,v
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }
    }
}