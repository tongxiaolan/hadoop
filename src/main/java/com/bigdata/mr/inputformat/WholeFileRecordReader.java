/**
 * Copyright (C), 2015-2020, 中发展信有限公司
 * FileName: WholeFileRecordReader
 * Author:   小懒
 * Date:     2020/7/9 21:53
 * Description:
 * History:
 * <author>           <version>          <desc>
 * 作者姓名            版本号              描述
 */
package com.bigdata.mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader 处理一个文件，直接把一个文件读取为一个k v
 *
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    /**
     * 是否读文件的标记
     */
    private boolean NOTREAD = true;

    /**
     * 读取文件的流
     */
    private FSDataInputStream inputStream;

    private FileSplit fs;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fs = (FileSplit) inputSplit;
        Path path = fs.getPath();
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        inputStream = fileSystem.open(path);

    }

    /**
     * 读取下一组kv值
     * 如果读到了，返回true,读完了，返回false
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(NOTREAD) {
            //读取过程
            // 读 key
            key.set(fs.getPath().toString());

            //读value
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf,0,buf.length);

            //读完了，因为读到了，所以返回true
            NOTREAD = false;
            return true;
        }else {
            return false;
        }
    }


    /**
     * 获取读到的Key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }


    /**
     * 获取读到的value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 读取进度 0.0-1.0
     * 因为一次全读完一个文件，所以只有未读和已读2个状态
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return NOTREAD ? 0 : 1;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}