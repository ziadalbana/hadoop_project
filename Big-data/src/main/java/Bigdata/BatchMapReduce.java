package Bigdata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class BatchMapReduce {
    public static class map extends Mapper
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
    public static class reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
            double sumCpu=0;
            double sumDisk=0;
            double sumRAM=0;
            double countMessage=0;
            double maxCPU=Double.MIN_VALUE;
            double maxDisk=Double.MIN_VALUE;
            double maxRAM=Double.MIN_VALUE;
            int maxCpuT=0;
            int maxDiskT=0;
            int maxRAMT=0;
            for (Text value:values) {
                String temp = value.toString();
                String[] vals=temp.split(" ");
                double tempCpu=Double.parseDouble(vals[0]);
                double tempDisk=Double.parseDouble(vals[1]);
                double tempRAM=Double.parseDouble(vals[2]);
                int time=Integer.parseInt(vals[3]);
                if (maxCPU<tempCpu){
                    maxCPU=tempCpu;
                    maxCpuT=time;
                }
                if(maxDisk<tempDisk){
                    maxDisk=tempDisk;
                    maxDiskT=time;
                }
                if (maxRAM<tempRAM){
                    maxRAM=tempRAM;
                    maxRAMT=time;
                }
                sumCpu+=tempCpu;
                sumDisk+=tempDisk;
                sumRAM+=tempRAM;
                countMessage++;
            }
            sumCpu/=countMessage;
            sumDisk/=countMessage;
            sumRAM/=countMessage;
            String result=","+sumCpu+","+maxCPU+","+maxCpuT+","+sumDisk+","+maxDisk+","+maxDiskT+","+sumRAM+","+maxRAM+","+maxRAMT+","+countMessage;
            output.write(key, new Text(result));
        }
    }

}
