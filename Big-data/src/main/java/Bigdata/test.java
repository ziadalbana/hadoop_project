package Bigdata;

import Bigdata.BatchView.BatchView;
import Bigdata.RealTimeView.RealTimeView;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;


// Hadoop stuff
import org.apache.hadoop.fs.Path;

// Generic Parquet dependencies
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;

// Avro->Parquet dependencies
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;
public class test {

    public static void main(String[] args) throws IOException {
//        Path dataFile = new Path("/tmp/demo.snappy.parquet");

       ArrayList<String> s= (ArrayList<String>) Files.readAllLines(Paths.get("2022-04-12"));
       RealTimeView r=new RealTimeView();
       r.calculate(s,"2022-04-12");
//        BatchView b=new BatchView();
//        b.createBatch("2022-04-10");

    }
}
