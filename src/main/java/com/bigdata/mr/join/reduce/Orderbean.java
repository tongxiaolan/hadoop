package com.bigdata.mr.join.reduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class Orderbean implements WritableComparable<Orderbean> {

    private String id;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public String toString() {
        return id + "/t"  + amount + "/t" + pname;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public int compareTo(Orderbean o) {
        int compare = this.pid.compareTo(o.pid);
        if (compare == 0) {
            return o.pname.compareTo(this.pname);
        }else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dataInput.readUTF();
        dataInput.readUTF();
        dataInput.readInt();
        dataInput.readUTF();
    }
}