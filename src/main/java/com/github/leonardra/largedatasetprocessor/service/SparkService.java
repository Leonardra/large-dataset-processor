package com.github.leonardra.largedatasetprocessor.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;


@Service
public class SparkService {

    private final SparkSession sparkSession;

    @Value("${database.url}")
    private String databaseUrl;

    public SparkService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }


    @Async
    public Dataset<Row> readFromTable(){

        String sqlQuery = "banks";
        return sparkSession.read()
                .format("jdbc")
                .option("url", databaseUrl)
                .option("user", "sa")
                .option("password", "password123")
                .option("partitionColumn", "Date")
                .option("numPartitions", "5")
                .option("fetchsize", "1000")
                .option("lowerBound", "2022-01-01")
                .option("upperBound", "2022-12-31")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("dbtable",sqlQuery)
                .load();
    }


    @Async
    public void writeIntoTable(Dataset<Row> rows){

        String sqlQuery = "banks";
        Dataset<Row> rowsWithIdColumn = rows.withColumn("id", functions.monotonically_increasing_id());

        rowsWithIdColumn.write()
                .format("jdbc")
                .option("url", databaseUrl)
                .option("user", "sa")
                .option("password", "password123")
                .option("fetchsize", "1000")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("dbtable","new_banks")
                .save();
    }

    @Async
    public void writeIntoCollection(Dataset<Row> rows){

        String mongoUri = "mongodb://localhost:27017/";
        Dataset<Row> rowsWithIdColumn = rows.withColumn("id", functions.monotonically_increasing_id());
       Dataset<Row> groupedRow = rowsWithIdColumn.groupBy("Location").agg(
               functions.collect_list(
                       functions.struct("Domain","Value", "Location", "Transaction_count"
                       ).as("settlementBreakdowns")
               )
       );

        groupedRow.write()
                .format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.output.uri", mongoUri)
                .option("spark.mongodb.output.database", "large_data")
                .option("spark.mongodb.output.collection", "banks")
                .mode("append") // Change to "overwrite" or "ignore" as needed
                .save();
    }


}
