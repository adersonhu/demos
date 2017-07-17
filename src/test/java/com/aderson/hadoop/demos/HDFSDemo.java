package com.aderson.hadoop.demos;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HDFSDemo {
    
    @Test
    public void add(){
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://:9000"), new Configuration());
            InputStream in = fs.open(new Path("/input/abc.txt"));
            OutputStream out = new FileOutputStream("F://Temp");
            IOUtils.copy(in, out, 4096);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
