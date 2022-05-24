package Bigdata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser;

import java.util.ArrayList;
import java.util.Arrays; 
@Component
public class ScheduledTasks {

	private static int index = 0 ;
	private static ArrayList<String> messages ;

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

	}
	
	private static String getTimeIn_HH_MM(String s){
		JSONObject jsonObject=(JSONObject) JSONValue.parse(s);  
		long unixSeconds= (long) jsonObject.get("Timestamp");  

		Date date = new java.util.Date(unixSeconds*1000L); 
		// the format of your date
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("hh:mm"); 
		String formattedDate = sdf.format(date);
		return formattedDate ;
	}
	
	@Scheduled(cron = "0 0 12 * * ?")
	public void  readFile() throws IOException{
//		BufferedReader bufReader = new BufferedReader(new FileReader("C:\\Users\\future\\Desktop\\2022-04-10.txt"));
//		messages = new ArrayList<>();
//		String line = bufReader.readLine();
//		while (line != null) {
//			messages.add(line);
//			line = bufReader.readLine(); 
//			} 
//		bufReader.close();
		
		String path = "hdfs://localhost:9000//user//hiberstack//messages//" + java.time.LocalDate.now() ;
		String file = "function(path)" ;
		messages = new ArrayList<>(Arrays.asList(file.split("\\R")));

		return;
	}
	
	
	


	
}