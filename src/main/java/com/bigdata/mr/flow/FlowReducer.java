/**
 * Copyright (C), 2015-2020, 中发展信有限公司
 * FileName: FlowReducer
 * Author:   小懒
 * Date:     2020/7/9 20:08
 * Description:
 * History:
 * <author>           <version>          <desc>
 * 作者姓名            版本号              描述
 */
package com.bigdata.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFLow = 0;
        for (FlowBean value : values) {
            sumUpFlow = value.getUpFlow();
            sumDownFLow = value.getDownFlow();
        }
        flowBean.set(sumUpFlow,sumDownFLow);
        context.write(key,flowBean);
    }
}