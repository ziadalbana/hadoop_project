package Bigdata;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class hdfsOperation {
    public  FileSystem configureFileSystem() {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.setBoolean("dfs.support.append", true);
            Path coreSite = new Path("/home/hiberstack/hadoop/hadoop-3.2.2/etc/hadoop/core-site.xml");
            Path hdfsSite = new Path("/home/hiberstack/hadoop/hadoop-3.2.2/etc/hadoop/hdfs-site.xml");
            conf.addResource(coreSite);
            conf.addResource(hdfsSite);
            fileSystem = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println("Error occurred while configuring FileSystem");
        }
        return fileSystem;
    }
    public  void createDirectory(String directoryName) throws IOException {
        FileSystem fileSystem = configureFileSystem();
        Path path = new Path(directoryName);
        fileSystem.mkdirs(path);
    }
    public  boolean checkDirectoryExist(String directoryName)throws IOException{
        FileSystem fileSystem = configureFileSystem();
        // Check if the file already exists
        Path path = new Path("/user/hiberstack/"+directoryName);
        if (fileSystem.exists(path)) {
            return true;
        }
        return false;
    }
    public  void writeFileToHDFS(String filePath,String fileName,ArrayList<String>jsons) {
        FileSystem fileSystem = configureFileSystem();
        //Create a path
        try {
        Path hdfsWritePath = new Path(filePath+ fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        for (String s:jsons) {
            bufferedWriter.write(s);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        fileSystem.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public  void appendToHDFSFile(String filePath,String fileName,ArrayList<String> jsons) throws IOException {
        FileSystem fileSystem = configureFileSystem();
        //Create a path
        if(!Boolean.parseBoolean(fileSystem.getConf().get("dfs.support.append"))){
            System.out.println("error");
            return;
        }
        Path hdfsWritePath = new Path(filePath+ fileName);
//        FSDataOutputStream fs_append = fileSystem.append(hdfsWritePath);
//        PrintWriter writer = new PrintWriter(fs_append);
//        writer.append(jsons);
//        writer.flush();
//        fs_append.hflush();
//        writer.close();
//        fs_append.close();
//
        FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsWritePath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        for (String s:jsons) {
            bufferedWriter.write(s);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        fileSystem.close();
    }
    public boolean DeleteFile(String dest) throws IOException {
        FileSystem fileSystem = configureFileSystem();
        Path hdfsPath = new Path(dest);
        return fileSystem.delete(hdfsPath,true);
    }
    public String ReadFile( String dest) {
        FileSystem fileSystem = configureFileSystem();
        Path hdfsReadPath = new Path(dest);
        String out= null;
        try {
            FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
            out = IOUtils.toString(inputStream, "UTF-8");
            inputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return out;
    }
    public  List<String> getAllFilePath(Path filePath)  {
        FileSystem fs = configureFileSystem();
        List<String> fileList = new ArrayList();
        FileStatus[] fileStatus ;
        try {
            fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllFilePath(fileStat.getPath()));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileList;
    }
}
