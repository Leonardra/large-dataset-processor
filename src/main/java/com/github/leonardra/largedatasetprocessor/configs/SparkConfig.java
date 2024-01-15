package com.github.leonardra.largedatasetprocessor.configs;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    public SparkConf sparkConf(){
        return new SparkConf()
                .setMaster("local[*]")
                .setAppName("bank-set");
    }


    @Bean
    public SparkSession sparkSession(){
        return SparkSession.builder().sparkContext(new SparkContext(sparkConf())).getOrCreate();
    }
}
