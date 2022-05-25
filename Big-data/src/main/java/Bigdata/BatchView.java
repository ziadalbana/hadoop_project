package Bigdata;

import Bigdata.phase2.MapRudece;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class BatchView {

    private static Configuration conf;
    private hdfsOperation operation=new hdfsOperation();
    public void createBatch(String folderName){
        dayPreprocessing(folderName);
        Path hdfsReadPath = new Path("hdfs://localhost:9000//user//hiberstack//minFiles");
        ArrayList<String> filePaths= (ArrayList<String>) operation.getAllFilePath(hdfsReadPath);
        for (String fp:filePaths) {
            ArrayList<serviceResult> results=analysis(fp);
        }
    }
    public void dayPreprocessing(String folderName){
        System.setProperty("HADOOP_USER_NAME", "hiberstack");
        String file=operation.ReadFile("hdfs://localhost:9000//user//hiberstack//messages//"+folderName);
        String[] lines=file.split("\n");
        String oldTime="";
        String timeFormated="";
        ArrayList<String> messages=new ArrayList<>();
        for (String s:lines) {
            JSONObject m= null;
            try {
                m = new JSONObject(s);
                int time = (int) m.get("Timestamp");
                timeFormated = getTime(time);
                if (oldTime == "" || oldTime.equals(timeFormated)) {
                    oldTime=timeFormated;
                    messages.add(s);
                } else {
                    operation.writeFileToHDFS("/user/hiberstack/minFiles/", oldTime, messages);
                    oldTime = timeFormated;
                    messages = new ArrayList<>();
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
        if (messages.size() > 0) {
            operation.writeFileToHDFS("/user/hiberstack/minFiles/", timeFormated, messages);
            System.out.println(messages.size());
        }
    }
    public ArrayList<serviceResult> analysis(String fileminPath){
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
            FileInputFormat.addInputPath(job, new Path(fileminPath));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000//user//hiberstack//output"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getResults(fileminPath.substring(fileminPath.lastIndexOf("/")));
    }
    private ArrayList<serviceResult> getResults(String batchName){
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
        operation.writeFileToHDFS("hdfs://localhost:9000//user//hiberstack//batchViews//",batchName,latestWrite);
        operation.writeFileToHDFS("hdfs://localhost:9000//user//hiberstack//latest//","latestFile",latestWrite);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return results;
    }
    private String getTime(double time){
        Date date = new java.util.Date((long) (time*1000L));
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm");
        return sdf.format(date);
    }
//    public static class CustomPathFilter implements PathFilter {
//
//        @Override
//        public boolean accept(Path path) {
//            //you can implement your logic for finding the valid range of paths here.
//            //The valid range of dates and days for directories and files can be input
//            //as arguments to the job.
//            //Return true if you find a match or else return false.
//            System.out.println(path.getName());
//            String fileName=conf.get("fileName");
//            if(path.getName().compareTo(fileName)==0) return true;
//            return false;
//        }
//    }

}
