package Bigdata.phase2;

import Bigdata.hdfsOperation;
import Bigdata.serviceResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;


public class MapRudece {
    static Configuration conf;
    hdfsOperation operation=new hdfsOperation();
    public ArrayList<serviceResult> analysis(String from, String to){
        System.setProperty("HADOOP_USER_NAME", "hiberstack");
        conf = new Configuration();
        conf.set("from",from);
        conf.set("to",to);
        Job job = null;
        try {
            operation.DeleteFile("hdfs://localhost:9000//user//hiberstack//output");
            job = Job.getInstance(conf, " count");
            job.setJarByClass(MapRudece.class);
            job.setMapperClass(mapper.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000//user//hiberstack//messages//*//"));
            FileInputFormat.setInputPathFilter(job, CustomPathFilter.class);
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000//user//hiberstack//output"));
            job.waitForCompletion(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return getResults();
    }
    private ArrayList<serviceResult> getResults(){
        ArrayList<serviceResult>  Result=new ArrayList<>();
        String results=operation.ReadFile("hdfs://localhost:9000//user//hiberstack//output//part-r-00000");
        String[] lines=results.split("\\n");
        for (int i = 0; i < lines.length; i++) {
            System.out.println(lines[i]);
            String[] line=lines[i].split(",");
            serviceResult m=new serviceResult(
                    line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10]);
            Result.add(m);
        }
        return Result;
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
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
    public static class mapper extends Mapper
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
    public static class CustomPathFilter implements PathFilter {

        @Override
        public boolean accept(Path path) {
            //you can implement your logic for finding the valid range of paths here.
            //The valid range of dates and days for directories and files can be input
            //as arguments to the job.
            //Return true if you find a match or else return false.
            System.out.println(path.getName());
            String from=conf.get("from");
            String to=conf.get("to");
            if(path.getName().compareTo(from)>0&&path.getName().compareTo(to)<=0) return true;
            return false;
        }
    }
}
