package com.aderson.hadoop.demos.reduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable(0);
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context
            ) throws IOException, InterruptedException {
        values.forEach(value->result.set(result.get()+value.get()));
        context.write(key, result);
    }
}
