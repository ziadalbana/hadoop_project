package Bigdata;

import java.util.*;

import java.io.IOException;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.boot.jackson.JsonObjectDeserializer;

public class mapreduce {
    //Mapper class
    public static class EMapper extends MapReduceBase implements
            Mapper<LongWritable,/*Input key Type */
                    Text,                /*Input value Type*/
                    Text,                /*Output key Type*/
                    Text>        /*Output value Type*/ {
        //Map function
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
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
                output.collect(new Text(service), new Text(result));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        //Reducer class
        public static class EReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

            //Reduce function
            public void reduce(Text key, Iterator<Text> values,
                               OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
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
                output.collect(key, new Text(result));
            }
        }
    }
}
