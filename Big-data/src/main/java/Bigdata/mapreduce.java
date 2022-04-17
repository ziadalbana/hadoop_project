package Bigdata;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

public class mapreduce {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        System.setProperty("hadoop.home.dir", "/");
//        System.setProperty("HADOOP_USER_NAME", "hiberstack");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, " count");
        job.setJarByClass(mapreduce.class);
        job.setMapperClass(EMapper.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class Reduce extends  Reducer<Text, Text, Text, Text> {
            public void reduce(Text key, Iterator<Text> values, Context output) throws IOException, InterruptedException {
                double sumCpu=0;
                double sumDisk=0;
                double sumRAM=0;
                double countMessage=0;
                double maxCPU=Double.MIN_VALUE;
                double maxDisk=Double.MIN_VALUE;
                double maxRAM=Double.MIN_VALUE;
                Date maxCpuT=new Date();
                Date maxDiskT=new Date();
                Date maxRAMT=new Date();
                while (values.hasNext()) {
                    String temp = values.next().toString();
                    String[] vals=temp.split(" ");
                    double tempCpu=Double.parseDouble(vals[0]);
                    double tempDisk=Double.parseDouble(vals[1]);
                    double tempRAM=Double.parseDouble(vals[3]);
                    int time=Integer.parseInt(vals[3]);
                    if (maxCPU<tempCpu){
                        maxCPU=tempCpu;
                        maxCpuT=new java.util.Date((long)time*1000);
                    }
                    if(maxDisk<tempDisk){
                        maxDisk=tempDisk;
                        maxDiskT=new java.util.Date((long)time*1000);
                    }
                    if (maxRAM<tempRAM){
                        maxRAM=tempRAM;
                        maxCpuT=new java.util.Date((long)time*1000);
                    }
                    sumCpu+=tempCpu;
                    sumDisk+=tempDisk;
                    sumRAM+=tempRAM;
                    countMessage++;
                }
                sumCpu/=countMessage;
                sumDisk/=countMessage;
                sumRAM/=countMessage;
                String result=sumCpu+" "+sumDisk+" "+sumRAM+" "+maxCpuT+" "+maxDiskT+" "+maxRAMT+" "+countMessage;
                output.write(key, new Text(result));
            }
        }
    public static class EMapper extends Mapper
            <LongWritable,/*Input key Type */
                    Text,                /*Input value Type*/
                    Text,                /*Output key Type*/
                    Text>        /*Output value Type*/
    {
        //Map function
        public void map(LongWritable key, Text value, Context output)  {
            String line = value.toString().replaceAll("\'","\"");
            try {
                JSONObject obj = new JSONObject(line);
                String service=obj.getString("serviceName");
                double cpuU=obj.getDouble("CPU");
                JSONObject disk=obj.getJSONObject("Disk");
                double diskU=(disk.getDouble("Total")-disk.getDouble("Free"))/disk.getDouble("Total");
                JSONObject ram=obj.getJSONObject("RAM");
                double ramU=(ram.getDouble("Total")-ram.getDouble("Free"))/ram.getDouble("Total");
                int timeStamp=obj.getInt("Timestamp");
                String result=cpuU+" "+diskU+" "+ramU+" "+timeStamp;
                output.write(new Text(service), new Text(result));
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

