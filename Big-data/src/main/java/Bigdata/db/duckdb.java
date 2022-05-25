package Bigdata.db;

import javax.management.Query;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class duckdb {
    public ArrayList<String> Query(String from , String to) throws ClassNotFoundException, SQLException {
        //"yyyy-mm-dd-hh-mm"
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        ResultSet payload = stmt.executeQuery("SELECT * FROM " +  from + ".parquet" + ";") ;

        while (payload.next()){
            // Print each column here
            System.out.println(payload.getString("id"));
            System.out.println(payload.getString("first"));
            System.out.println(payload.getString("second"));

        }

        return new ArrayList<String>() ;
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        duckdb db = new duckdb() ;
        ArrayList<String> res = db.Query("file","vv");
    }
}
