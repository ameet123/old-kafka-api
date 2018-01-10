package com.anthem.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RetrogradeReaderApp implements CommandLineRunner {
    @Autowired
    private RetrogradeBeanConfig beanConfig;

    public static void main(String[] args) {
        SpringApplication.run(RetrogradeReaderApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Running config instance...");
        beanConfig.startFetcherThreads();
//        new KafkaConfigInstance().run(1);
        Thread.sleep(30000);
        System.out.println("END... shouldn't see this");
    }
}
