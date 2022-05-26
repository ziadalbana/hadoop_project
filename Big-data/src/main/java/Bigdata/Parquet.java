package Bigdata;

import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;

public class Parquet {

    public  void write(String fileName, List<serviceResult> records) {

        try (ParquetWriter<serviceResult> writer = AvroParquetWriter.<serviceResult>builder(new Path(fileName))
                .withSchema(ReflectData.AllowNull.get().getSchema(serviceResult.class))
                .withDataModel(ReflectData.get())
                .withConf(new Configuration())
                .withCompressionCodec(SNAPPY)
                .withWriteMode(OVERWRITE)
                .build()) {

            for (serviceResult s : records)
                writer.write(s);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
