package com.weds.database.syncserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages={"com.weds.database.syncserver","com.weds.framework.mq.rabbitmq"})
public class SyncServerApplication {
	public static void main(String[] args) {
		SpringApplication.run(SyncServerApplication.class, args);
	}
}
