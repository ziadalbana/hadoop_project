package Bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Pattern;

public class RealTimeView {
    private  Parquet parquet=new Parquet();


    private static final Pattern SPACE = Pattern.compile(" ");

    SparkSession spark;


    public RealTimeView() {

        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RealTimeView")
                .getOrCreate();

    }

    public void calculate(List<String> records, String fileName){

        JavaRDD<String> lines = spark.createDataset(records, Encoders.STRING()).toJavaRDD();

        JavaPairRDD<String, String> tuples = lines.mapToPair(s -> {

            JSONObject obj = new JSONObject(s);
            JSONObject diskObj = obj.getJSONObject("Disk");
            JSONObject ramObj = obj.getJSONObject("RAM");


            String service = obj.getString("serviceName");
            int timeStamp = obj.getInt("Timestamp");
            double cpu = obj.getDouble("CPU");

            double disk = (diskObj.getDouble("Total") - diskObj.getDouble("Free")) / diskObj.getDouble("Total");

            double ram = (ramObj.getDouble("Total") - ramObj.getDouble("Free")) / ramObj.getDouble("Total");

            String result = cpu + " " + disk + " " + ram + " " + timeStamp;

            return new Tuple2<>(service, result);
        });


        JavaRDD<serviceResult> counts = tuples.groupByKey().map(values -> {

            double sumCpu = 0;
            double sumDisk = 0;
            double sumRAM = 0;
            int countMessage = 0;

            double maxCPU = Double.MIN_VALUE;
            double maxDisk = Double.MIN_VALUE;
            double maxRAM = Double.MIN_VALUE;
            int maxCpuT = 0;
            int maxDiskT = 0;
            int maxRAMT = 0;

            for (String s : values._2) {

                String[] vals = s.split(" ");

                double tempCpu = Double.parseDouble(vals[0]);
                double tempDisk = Double.parseDouble(vals[1]);
                double tempRAM = Double.parseDouble(vals[2]);
                int time = Integer.parseInt(vals[3]);

                if (maxCPU < tempCpu) {
                    maxCPU = tempCpu;
                    maxCpuT = time;
                }

                if (maxDisk < tempDisk) {
                    maxDisk = tempDisk;
                    maxDiskT = time;
                }

                if (maxRAM < tempRAM) {
                    maxRAM = tempRAM;
                    maxRAMT = time;
                }

                sumCpu += tempCpu;
                sumDisk += tempDisk;
                sumRAM += tempRAM;
                countMessage++;
            }

            sumCpu /= countMessage;
            sumDisk /= countMessage;
            sumRAM /= countMessage;

            return new serviceResult(values._1,
                    fileName,
                    String.valueOf(sumCpu),
                    String.valueOf(maxCPU),
                    String.valueOf(maxCpuT),
                    String.valueOf(sumDisk),
                    String.valueOf(maxDisk),
                    String.valueOf(maxDiskT),
                    String.valueOf(sumRAM),
                    String.valueOf(maxRAM),
                    String.valueOf(maxRAMT),
                    String.valueOf(countMessage));
        });

        List<serviceResult> output = counts.collect();
        parquet.write("realTime/"+fileName+".parquet",output);

    }
}
