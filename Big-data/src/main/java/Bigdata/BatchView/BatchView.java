package Bigdata.BatchView;

import Bigdata.Parquet;
import Bigdata.Utilits.hdfsOperation;
import Bigdata.serviceResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class BatchView {

    private static Configuration conf;
    private hdfsOperation operation=new hdfsOperation();
    private Parquet parquet=new Parquet();
    public void createBatch(String folderName){
        System.setProperty("HADOOP_USER_NAME", "hiberstack");
        ArrayList<serviceResult> results=analysis("hdfs://localhost:9000//user//hiberstack//messages//"+folderName);
        parquet.write("batchView/"+folderName+".parquet",results);
    }

    public ArrayList<serviceResult> analysis(String filePath){
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
//            job.setOutputFormatClass(AvroParquetOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(filePath));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000//user//hiberstack//output"));
            job.waitForCompletion(true);
        } catch (Exception e) {
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
                    line[0],line[1],Double.parseDouble(line[2]),Double.parseDouble(line[3]),Double.parseDouble(line[4]),Double.parseDouble(line[5]),Double.parseDouble(line[6]),Double.parseDouble(line[7]),Double.parseDouble(line[8]),Double.parseDouble(line[9]),Double.parseDouble(line[10]),Double.parseDouble(line[11]));
            Result.add(m);
        }
        return Result;
    }
    private String getTime(double time){
        Date date = new java.util.Date((long) (time*1000L));
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm");
        return sdf.format(date);
    }

}
