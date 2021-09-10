package com.cloudera.examples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaTransactionalReplicator {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTransactionalReplicator.class, args);
	}

}
