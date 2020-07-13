package com.bigdata.mr.join.reduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class RJComparator extends WritableComparator {

    public RJComparator() {
        super(Orderbean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Orderbean oa = (Orderbean) a;
        Orderbean ob = (Orderbean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}