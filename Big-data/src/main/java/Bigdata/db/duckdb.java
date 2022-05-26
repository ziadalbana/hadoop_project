package Bigdata.db;

import Bigdata.serviceResult ;

import javax.swing.text.html.parser.Entity;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class duckdb {

    private Connection conn ;
    private Statement stmt ;
    private ArrayList<serviceResult> result ;
    private HashMap<String,serviceResult> map ;

    // Constructor
    public  duckdb() throws ClassNotFoundException, SQLException {

        Class.forName("org.duckdb.DuckDBDriver");
        conn = DriverManager.getConnection("jdbc:duckdb:");
        stmt = conn.createStatement();

        result = new ArrayList<serviceResult>(4);
        map = new HashMap<>();

        // Initializing the four services.
        for (int i = 1 ; i <= 4 ; i++){
            String service = "service-" + Integer.toString(i) ;
            map.put(service, new serviceResult()) ;
            map.get(service).setService(service);
        }

    }


    private Timestamp convertToTimeStamp(String time){
        String[] dayAndTime = time.split("-") ;
        String timeStampAsString = dayAndTime[0] + "-" + dayAndTime[1] + "-" +  dayAndTime[2] + " " + dayAndTime[3] + ":" + dayAndTime[4]+":00" ;
        System.out.println(timeStampAsString);
        Timestamp ts = Timestamp.valueOf(timeStampAsString) ;
        return ts ;
    }

    // Dispatcher
    public HashMap<String,serviceResult> QueryAPI(String from , String to) throws ClassNotFoundException, SQLException {

        String day = convertToTimeStamp(from).toString().split(" ")[0] ;
        String currentday = LocalDate.now().toString();

        if (day.equals(currentday))
            return realTimeViewQuery(day,from,to) ;
        else
            return batchViewQuery(day,from,to) ;

    }


    private HashMap<String,serviceResult> realTimeViewQuery(String day ,String from, String to) throws SQLException {

        Timestamp fromTimeStamp = convertToTimeStamp(from) ;
        Timestamp toTimeStamp = convertToTimeStamp(to) ;
        String tablePath = "" ;

        while (fromTimeStamp.before(toTimeStamp) || fromTimeStamp.equals(toTimeStamp)){

            String fileName = day + "-" + getFileName(fromTimeStamp) ;

            tablePath = "'" + "realtimeview/" + fileName + ".parquet" + "' " ;

            ResultSet payload = stmt.executeQuery("SELECT * FROM " +  tablePath + ";") ;
            processPayload(payload,fromTimeStamp,toTimeStamp);

            fromTimeStamp.setTime(fromTimeStamp.getTime() + TimeUnit.SECONDS.toSeconds(1000 * 60));

        }

        return map ;
    }

    private String getFileName(Timestamp ts){
        String hours = ts.getHours() + "" ;
        if (hours.length() != 2) hours = "0" + hours ;
        String minutes = ts.getMinutes() + "" ;
        if (minutes.length() != 2 ) minutes = "0" + minutes ;
        return hours + "-" + minutes ;
    }

    private HashMap<String,serviceResult> batchViewQuery(String day, String from , String to) throws SQLException {

        String tablePath = "'" + "batchview/" + day + ".parquet" + "' " ;

        ResultSet payload = stmt.executeQuery("SELECT * FROM " +  tablePath + ";") ;
        Timestamp fromTimeStamp = convertToTimeStamp(from) ;
        Timestamp toTimeStamp = convertToTimeStamp(to) ;

        processPayload(payload,fromTimeStamp,toTimeStamp);

        return map ;
    }

    private void processPayload(ResultSet payload,Timestamp fromTimeStamp , Timestamp toTimeStamp) throws SQLException {

        while (payload.next()){

            Timestamp currentTimeStamp = convertToTimeStamp(payload.getString("timeStamp").trim()) ;

            boolean lowerBound = currentTimeStamp.equals(fromTimeStamp) || currentTimeStamp.after(fromTimeStamp) ;
            boolean upperBound = currentTimeStamp.equals(toTimeStamp) || currentTimeStamp.before(toTimeStamp) ;

            if (!(lowerBound && upperBound)) continue;

            String service = payload.getString("service") ;
            Double cpu = payload.getDouble("Cpu") ;
            Double cpuM = payload.getDouble("CpuM") ;
            Double cpuMT = payload.getDouble("CpuMT") ;
            Double disk = payload.getDouble("Disk") ;
            Double diskM = payload.getDouble("DiskM") ;
            Double diskMT = payload.getDouble("DiskMT") ;
            Double ram = payload.getDouble("Ram") ;
            Double ramM = payload.getDouble("RamM") ;
            Double ramMT = payload.getDouble("RamMT") ;
            Double count = payload.getDouble("count") ;

            serviceResult currentService = map.get(service);

            currentService.setCount(currentService.getCount() + count);
            currentService.setCpu(currentService.getCpu() + cpu);
            currentService.setRam(currentService.getRam() + ram);
            currentService.setDisk(currentService.getDisk() + disk);

            Double cpuMSoFar = currentService.getCpuM() ;
            Double diskMSoFar = currentService.getDiskM() ;
            Double ramMSoFar = currentService.getRamM() ;


            if (cpuMSoFar < cpuM ){
                currentService.setCpuM(cpuM);
                currentService.setCpuMT(cpuMT);
            }

            if (diskMSoFar < diskM ){
                currentService.setDiskM(diskM);
                currentService.setDiskMT(diskMT);
            }

            if (ramMSoFar < ramM){
                currentService.setRamM(ramM);
                currentService.setRamMT(ramMT);
            }

        }

        // Updating Cpu, Ram and Disk values.
        for (Map.Entry<String,serviceResult> K : map.entrySet() ){
            serviceResult service = K.getValue() ;
            service.setCpu(service.getCpu() / service.getCount() );
            service.setRam(service.getRam() / service.getCount() );
            service.setDisk(service.getDisk() / service.getCount() );
        }

        return ;

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        duckdb db = new duckdb() ;
        Timestamp ts = db.convertToTimeStamp("2022-12-10-09-00") ;
        Timestamp ts2 = db.convertToTimeStamp("2022-12-10-10-00") ;
         while(ts2.after(ts)){
            ts.setTime(ts.getTime() + TimeUnit.MINUTES.toMinutes(1000 * 60));
            System.out.println(db.getFileName(ts)) ;
        }
        return ;
    }
}
