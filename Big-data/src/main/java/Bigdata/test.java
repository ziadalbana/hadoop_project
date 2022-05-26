package Bigdata;

import Bigdata.BatchView.BatchView;
import Bigdata.RealTimeView.RealTimeView;
import Bigdata.db.duckdb;
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
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
//        Path dataFile = new Path("/tmp/demo.snappy.parquet");

//       ArrayList<String> s= (ArrayList<String>) Files.readAllLines(Paths.get("2022-04-12"));
//       RealTimeView r=new RealTimeView();
//       r.calculate(s,"2022-04-12");
//        BatchView b=new BatchView();
//        b.createBatch("2022-04-12");

            duckdb db=new duckdb();
            HashMap<String,serviceResult>result=db.QueryAPI("2022-04-11-00-00","2022-04-11-16-37");
        for (Map.Entry<String,serviceResult> K : result.entrySet() ){
            System.out.println(K.getValue().toString());
        }

    }
}
