package com.github.leonardra.largedatasetprocessor.controller;

import com.github.leonardra.largedatasetprocessor.service.SparkService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/jobs/")
public class SparkController {


    private final SparkService sparkService;

    public SparkController(SparkService sparkService) {
        this.sparkService = sparkService;
    }


    @GetMapping("")
    public ResponseEntity<?> getBanks(){
        Dataset<Row> result = sparkService.readFromTable().persist(StorageLevel.MEMORY_ONLY_SER());
        sparkService.writeIntoCollection(result);

        return new ResponseEntity<>(HttpStatus.FOUND);
    }
}
