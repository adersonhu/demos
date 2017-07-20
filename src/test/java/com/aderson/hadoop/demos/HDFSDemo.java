package com.aderson.hadoop.demos;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
    FileSystem fs = null;
    
    InputStream in = null;
    
    OutputStream out = null;
    
    @Before
    public void init(){
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.31.140:9000"), new Configuration(),"root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void add(){
        try {
            in = new FileInputStream(new File("F:/Temp/document.txt"));
            out = fs.create(new Path("/input/document.txt"));
            IOUtils.copy(in, out, 4096);
        } catch ( IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void get(){
        try {
            in = fs.open(new Path("/input/core-site.xml"));
            out = new FileOutputStream(new File("F:\\Temp\\core-site.xml"));
            IOUtils.copy(in, out, 4096);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @After
    public void destory(){
        try {
            if(in != null){
                in.close();
            }
            if(out != null){
                out.close();
            }
            if(fs != null){
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    
}
