package com.software.cdc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class CdcApplication {

	public static void main(String[] args) {
		SpringApplication.run(CdcApplication.class, args);
	}

}
