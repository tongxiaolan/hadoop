package com.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义炸列函数  一进多出
 * @author 小懒
 * @create 2020/7/30
 * @since 1.0.0
 */
public class MyUDTF extends GenericUDTF{
    //写出的数据
    private List<String> dataList = new ArrayList<>();

    /**
     * 初始化方法，不会提示重写该方法，但是必须写，否则执行
     *  会报错（GenericUDTF的initialize()方法直接抛异常）
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 默认的输出列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");

        //输出结果数据类型，注意，所有的数据类型都要用PrimitiveObjectInspectorFactory来获取，下例为String
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 对每行数据调用一次  forward为父类写出数据方法。
     * @param objects 将传入的所有参数封装到其中
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {

//        1. 获取数据
        String data = objects[0].toString();
//        2. 获取分割符
        String splitKey = objects[1].toString();
//        3. 切分数据
        String[] words = data.split(splitKey);
//        4. 遍历放到集合中写出
        for (String word : words) {
            dataList.clear();
            dataList.add(word);
            forward(dataList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}