package com.bigdata.mr.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //该写法因为用orderId分组，所以只会写出不同的orderId的数据
        context.write(key,NullWritable.get());
        /* 计算结果
         *    0000001	Pdt_01	222.8
              0000002	Pdt_05	722.4
              0000003	Pdt_06	232.8
         */


        /* TODO
         * 遍历，这里特别注意：
         *  遍历的时候有2个变量，key和v 所以所有的key都会输出
         */

        /*for (NullWritable value : values) {
            context.write(key,value);
        }*/
        /*
            同样，使用迭代器也是同样的结果。
            这是因为mapreduce是以k,v来传递值的，当执行next方法的时候，
            会将key,value映射(序列化)到 k,v实例对象当中（OrderComparator 中写道的，会创建2个实例来接受数据）

         */
       /* Iterator<NullWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            context.write(key , iterator.next());
        }*/

        /* 计算结果
         *  0000002	Pdt_05	722.4
            0000002	Pdt_03	522.8
            0000002	Pdt_04	122.4
            0000001	Pdt_01	222.8
            0000001	Pdt_02	33.8
            0000003	Pdt_06	232.8
            0000003	Pdt_02	33.8
         */





    }
}