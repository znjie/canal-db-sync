package com.suncompass.tool.sz.sync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.suncompass.tool"})
public class SzDbSyncApplication {
    public static void main(String[] args) {
        SpringApplication.run(SzDbSyncApplication.class, args);
    }
}
