package com.bigdata.mr.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  用来分组，所以只比较订单id 订单id一致即在一组
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class OrderComparator extends WritableComparator{

    /**
     * 创建实例对象，用来序列化数据 key，如果没有创建的化，可能会出现没有实例来接受数据的情况
     * 所以必须写该方法！！！
     */
    public OrderComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}