package Bigdata.db;

import Bigdata.serviceResult ;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
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
        Timestamp ts = Timestamp.valueOf(timeStampAsString) ;
        return ts ;
    }

    // Dispatcher
    public HashMap<String,serviceResult> QueryAPI(String from , String to) throws ClassNotFoundException, SQLException {

        String day = convertToTimeStamp(from).toString().split(" ")[0] ;
        String currentday = LocalDate.now().toString();

        if (day.equals(currentday))
            return realTimeViewQuery(from,to) ;
        else
            return batchViewQuery(day,from,to) ;

    }


    private HashMap<String,serviceResult> realTimeViewQuery(String from, String to) throws SQLException {

        Timestamp fromTimeStamp = convertToTimeStamp(from) ;
        Timestamp toTimeStamp = convertToTimeStamp(to) ;
        String tablePath = "" ;

        while (fromTimeStamp.before(toTimeStamp) || fromTimeStamp.equals(toTimeStamp)){

            tablePath = "'" + "realtimeview/" + from + ".parquet" + "' " ;

            ResultSet payload = stmt.executeQuery("SELECT * FROM " +  tablePath + ";") ;
            processPayload(payload,fromTimeStamp,toTimeStamp);

            fromTimeStamp.setTime(fromTimeStamp.getTime() + TimeUnit.SECONDS.toSeconds(1000 * 60));

        }

        return map ;
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
            String cpu = payload.getString("Cpu") ;
            String cpuM = payload.getString("CpuM") ;
            String cpuMT = payload.getString("CpuMT") ;
            String disk = payload.getString("Disk") ;
            String diskM = payload.getString("DiskM") ;
            String diskMT = payload.getString("DiskMT") ;
            String ram = payload.getString("Ram") ;
            String ramM = payload.getString("RamM") ;
            String ramMT = payload.getString("RamMT") ;
            String count = payload.getString("count") ;

            serviceResult currentService = map.get(service);

            currentService.setCount(currentService.getCount() + count);
            currentService.setCount(currentService.getCpu() + cpu);
            currentService.setCount(currentService.getRam() + ram);

            String cpuMSoFar = currentService.getCpuM() ;
            String diskMSoFar = currentService.getDiskM() ;
            String ramMSoFar = currentService.getRamM() ;


            if (Double.valueOf(cpuMSoFar) < Double.valueOf(cpuM)){
                currentService.setCpuM(cpuM);
                currentService.setCpuMT(cpuMT);
            }

            if (Double.valueOf(diskMSoFar) < Double.valueOf(diskM)){
                currentService.setDiskM(cpuM);
                currentService.setDiskMT(cpuMT);
            }

            if (Double.valueOf(ramMSoFar) < Double.valueOf(ramM)){
                currentService.setCpuM(ramM);
                currentService.setCpuMT(ramMT);
            }

        }

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        duckdb db = new duckdb() ;

    }
}
