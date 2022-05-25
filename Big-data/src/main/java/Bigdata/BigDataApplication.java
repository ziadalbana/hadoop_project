package Bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BigDataApplication {

	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "hiberstack");
		SpringApplication.run(BigDataApplication.class, args);
	}

}
