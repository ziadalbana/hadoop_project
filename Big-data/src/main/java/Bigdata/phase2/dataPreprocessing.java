package Bigdata.phase2;

import Bigdata.hdfsOperation;
import com.google.gson.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class dataPreprocessing {
    hdfsOperation operation =new hdfsOperation();
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hiberstack");
        System.setProperty("hadoop.home.dir", "/");
        dataPreprocessing n=new dataPreprocessing();
        hdfsOperation operation =new hdfsOperation();
        n.reader();
    }
    public void reader() {
        InputStream is = null;
        try {
            File folder = new File("health_data");
            File[] listOfFiles = folder.listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                is = new FileInputStream(listOfFiles[i].getAbsolutePath());
                ArrayList<String> jsons = new ArrayList<>();
                Reader r = new InputStreamReader(is, "UTF-8");
                Gson gson = new GsonBuilder().create();
                JsonStreamParser p = new JsonStreamParser(r);
                String oldTime = "";
                String timeFormated = "";
                while (p.hasNext()) {
                    JsonElement e = p.next();
                    if (e.isJsonObject()) {
                        Map m = gson.fromJson(e, Map.class);
                        double time = (double) m.get("Timestamp");
                        timeFormated = getTime(time);
                        if (oldTime == "" || oldTime.equals(timeFormated)) {
                            oldTime=timeFormated;
                            jsons.add(e.toString());
                        } else {
                            operation.writeFileToHDFS("/user/hiberstack/messages/", timeFormated, jsons);
                            oldTime = timeFormated;
                            System.out.println(jsons.size());
                            jsons = new ArrayList<>();
                        }


                    }
                }
                if (jsons.size() > 0) {
                    operation.writeFileToHDFS("/user/hiberstack/messages/", timeFormated, jsons);
                    System.out.println(jsons.size());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
        public String getTime(double time){
        Date date = new java.util.Date((long) (time*1000L));
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }

}
