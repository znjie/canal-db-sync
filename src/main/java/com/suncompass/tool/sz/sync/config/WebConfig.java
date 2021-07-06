package com.suncompass.tool.sz.sync.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * WebConfig
 *
 * @author dengbq
 * @since 2020/9/15
 */
@Configuration
public class WebConfig {
    @Bean
    public RestTemplate restTemplate(){
        return new RestTemplate();
    }

//    @Primary
//    @Bean
//    public DataSource dataSource(DataSourceProperties properties) {
//        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
//    }
}