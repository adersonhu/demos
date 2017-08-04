package com.aderson.hadoop.demos.example;

import org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
    
    public static void main(String[] argv){
        int exitcode = -1;
        ProgramDriver pd = new ProgramDriver();
        try {
            pd.addClass("wordcount", WordCount.class, "word count");
            exitcode = pd.run(argv);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitcode);
    }
}
