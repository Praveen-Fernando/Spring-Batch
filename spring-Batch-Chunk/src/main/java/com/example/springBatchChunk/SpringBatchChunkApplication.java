package com.example.springBatchChunk;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchChunkApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchChunkApplication.class, args);
	}

}
