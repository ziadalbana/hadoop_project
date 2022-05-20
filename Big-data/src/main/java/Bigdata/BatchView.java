package Bigdata;

import Bigdata.phase2.MapRudece;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class BatchView {
    private Configuration conf;
    private hdfsOperation operation=new hdfsOperation();
    public ArrayList<serviceResult> analysis(String folderPath){
        System.setProperty("HADOOP_USER_NAME", "hiberstack");
        conf = new Configuration();
        Job job = null;
        try {
            operation.DeleteFile("hdfs://localhost:9000//user//hiberstack//output");
            job = Job.getInstance(conf, " count");
            job.setJarByClass(BatchMapReduce.class);
            job.setMapperClass(BatchMapReduce.map.class);
            job.setReducerClass(BatchMapReduce.reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000//"+folderPath));
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
        ArrayList<serviceResult> results=new ArrayList<>();
        ArrayList<String> latestWrite=new ArrayList<>();
        String outputFilePath="hdfs://localhost:9000//user//hiberstack//output//part-r-00000";
        String latestFilePath="latest//latestFilePath";
        try {
        String realTimeresults=operation.ReadFile(outputFilePath);
        String[] newlines=realTimeresults.split("\\n");
        String oldresults;
        if(operation.checkDirectoryExist(latestFilePath)){
            oldresults=operation.ReadFile(latestFilePath);
            String[] oldLines=oldresults.split("\\n");
            for (int i = 0; i < newlines.length; i++) {
                String[] newline=newlines[i].split(",");
                String[] oldLine=oldLines[i].split(",");
                serviceResult m=new serviceResult();
                m.combine(newline,oldLine);
                results.add(m);
                latestWrite.add(m.toString());
            }
        }else{
            for (int i = 0; i < newlines.length; i++) {
                System.out.println(newlines[i]);
                String[] line=newlines[i].split(",");
                serviceResult m=new serviceResult(
                        line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10]);
                results.add(m);
                latestWrite.add(m.toString());
            }
        }
        operation.writeFileToHDFS("hdfs://localhost:9000//user//hiberstack//latest//","latestFilePath",latestWrite);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

}
