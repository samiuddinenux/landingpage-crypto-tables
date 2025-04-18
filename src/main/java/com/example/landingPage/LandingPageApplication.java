package com.example.landingPage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@EnableScheduling

@SpringBootApplication
public class LandingPageApplication {

	public static void main(String[] args) {
		SpringApplication.run(LandingPageApplication.class, args);
	}

}
