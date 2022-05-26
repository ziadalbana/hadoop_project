package Bigdata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import Bigdata.RealTimeView.RealTimeView;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser;
import com.nimbusds.jose.util.DateUtils;

import Bigdata.BatchView.BatchView;
import Bigdata.Utilits.hdfsOperation;

import java.util.ArrayList;
import java.util.Arrays;
@Component
public class ScheduledTasks {

	private static int index = 0 ;
	private static ArrayList<String> messages ;
	private static String d = "" ;

	@Scheduled(fixedRate = 1000*60)
	public void performTask() throws IOException {
		ArrayList<String> ms = new ArrayList<String>();

		String msg = messages.get(index++) ;
		String t1 = getTimeIn_HH_MM(msg);

		ms.add(msg);

		while(true && index < messages.size()) {
			msg = messages.get(index++) ;
			String t2 = getTimeIn_HH_MM(msg);

			if(t1.equals(t2))
			{
				ms.add(msg);
			}
			else
			{
				index-- ;
				break ;
			}
		}

		//send ms list to spark
		RealTimeView rt = new RealTimeView();
		rt.calculate(ms, d);

	}

	private static String getTimeIn_HH_MM(String s){
		JSONObject jsonObject=(JSONObject) JSONValue.parse(s);
		long unixSeconds= (long) jsonObject.get("Timestamp");

		Date date = new java.util.Date(unixSeconds*1000L);
		// the format of your date
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm");
		String formattedDate = sdf.format(date);
		d = formattedDate ;
		return formattedDate ;
	}

	@Scheduled(cron = "0 0 12 * * ?")
	public void  readFile() throws IOException{

		String path = "hdfs://localhost:9000//user//hiberstack//messages//" + java.time.LocalDate.now() ;
		hdfsOperation h = new hdfsOperation() ;

		String file = h.ReadFile(path) ;

		// In string format
		LocalDate today = LocalDate.now();
		String yesterday = (today.minusDays(1)).format(DateTimeFormatter.ISO_DATE);
		// In string format
		BatchView b = new BatchView() ;
		b.createBatch(yesterday);

		return;
	}







}