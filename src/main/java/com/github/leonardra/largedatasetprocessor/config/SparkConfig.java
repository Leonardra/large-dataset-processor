package com.github.leonardra.largedatasetprocessor.config;


import org.apache.hadoop.fs.FileSystem;
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
                .setJars(new String[]{System.getProperty("user.home")+"/sqljdbc_11.2/enu/mssql-jdbc-11.2.0.jre8.jar", System.getProperty("user.home")+"/jars/large-dataset-processor.jar"})
                .setAppName("bank-set");
    }


    @Bean
    public SparkSession sparkSession(){
        SparkContext sparkContext = new SparkContext(sparkConf());
//        sparkContext.hadoopConfiguration().setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);
        return SparkSession.builder().sparkContext(sparkContext).getOrCreate();
    }
}
