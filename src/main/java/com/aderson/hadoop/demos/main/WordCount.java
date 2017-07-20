package com.aderson.hadoop.demos.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.aderson.hadoop.demos.mapper.WordCountMapper;
import com.aderson.hadoop.demos.reduce.WordCountReducer;

public class WordCount {
    
    
    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        
        if(otherArgs.length<2){
            System.out.println("Usage:WordCountMain <in>[<in>..] <out>");
            System.exit(2);
        }
        
        JobConf jobConf = new JobConf(WordCount.class);
//        jobConf.setMapperClass(WordCountMapper.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);
        
        
        for(int i=0;i<args.length-1;i++){
            TextInputFormat.addInputPath(jobConf, new Path(args[i]));
        }
        
        
        
        
        
    }
}
