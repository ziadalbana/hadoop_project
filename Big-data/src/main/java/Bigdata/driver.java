package Bigdata;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class driver {
    //Main function

    public static void main(String args[])throws Exception {
        JobConf conf = new JobConf(mapreduce.class);
        conf.setJobName("calculation");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(mapreduce.EMapper.class);
        conf.setCombinerClass(mapreduce.EMapper.EReduce.class);
        conf.setReducerClass(mapreduce.EMapper.EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path("health_0.json"));
        FileOutputFormat.setOutputPath(conf, new Path("output.txt"));
        JobClient.runJob(conf);
    }
}
