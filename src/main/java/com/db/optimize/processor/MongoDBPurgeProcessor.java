package com.db.optimize.processor;

import com.db.optimize.service.MongoDBPurgeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MongoDBPurgeProcessor {

    @Value("${mongodb.collection}")
    private String collection;

    @Autowired
    private MongoDBPurgeService mongoDBPurgeService;

    @Scheduled(cron = "0 00 00 * * ?",zone = "America/chicago") // 0-sec 00-min 00-hr *-day *-month ?-day of week
    public void scheduler(){
        mongoDBPurgeService.deleteDocsProcessor(collection);
    }

}
