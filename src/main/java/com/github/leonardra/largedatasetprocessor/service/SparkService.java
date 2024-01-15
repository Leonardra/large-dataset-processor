package com.github.leonardra.largedatasetprocessor.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jvnet.hk2.annotations.Service;
import org.springframework.beans.factory.annotation.Value;


@Service
public class SparkService {

    private final SparkSession sparkSession;

    @Value("${database.url}")
    private String databaseUrl;

    public SparkService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }


    public Dataset<Row> readFromTable(){

        String sqlQuery = "select top (1000) * from [bank_arena].[dbo].[banks]";
        return sparkSession.read()
                .format("jdbc")
                .option("url", databaseUrl)
                .option("user", "sa")
                .option("password", "password123")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("query",sqlQuery)
                .load();
    }
}
