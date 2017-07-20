package com.aderson.hadoop.demos.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
        Job job = new Job(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        for(int i=0;i<otherArgs.length-1;i++){
        	FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        
        
        
        
        
    }
}
