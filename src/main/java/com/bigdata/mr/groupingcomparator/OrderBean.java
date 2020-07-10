package com.bigdata.mr.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String productId;
    private double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + productId + "\t" + price;
    }

    /**
     * 先按照订单排序，然后按照价格倒叙排
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        int compare = o.orderId.compareTo(this.orderId);
        if (compare == 0) {
            return Double.compare(o.price, this.price);
        } else {
            return compare;
        }
     }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        productId = dataInput.readUTF();
        price = dataInput.readDouble();
    }
}