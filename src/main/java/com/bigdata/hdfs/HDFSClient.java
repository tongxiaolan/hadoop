package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * 操作hdfs文件
 * @author 小懒
 * @create 2020/7/8
 * @since 1.0.0
 */
public class HDFSClient {

    public static String uri = "hdfs://192.168.5.101:9000";

    public static void main(String[] args) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.5.101:9000"),new Configuration(),"hadoop");
        fs.copyFromLocalFile(new Path("d:\\words.txt"),new Path("/"));
        fs.close();
    }

    @Test
    public void put() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.5.101:9000"), new Configuration(), "hadoop");
        fs.copyFromLocalFile(new Path("d:\\words.txt"),new Path("/"));
        fs.close();
    }

    @Test
    public void get() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.5.101:9000"), new Configuration(), "hadoop");
        fs.copyToLocalFile(new Path("/wcoutput"),new Path("d:\\"));
        fs.close();

    }

    @Test
    public void rename() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration(), "hadoop");
        fs.rename(new Path("/words.txt"), new Path("/words1.txt"));
        fs.close();
    }


    @Test
    public void delete() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration(), "hadoop");
        fs.delete(new Path("/words1.txt"), true);
        fs.close();
    }

    @Test
    public void listFiles() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration(), "hadoop");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("================================");
            System.out.println(fileStatus.getPath()+"在");
            for (BlockLocation blockLocation : blockLocations) {
                for (String host : blockLocation.getHosts()) {
                    System.out.println(host);
                }
            }
        }
    }

    @Test
    public void listStatus() throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(uri), new Configuration(), "hadoop");
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isDirectory()) {
                System.out.println(fileStatus.getPath()+"is a dir");
            }else {
                System.out.println(fileStatus.getPath()+"is a file");
            }
        }


    }







}