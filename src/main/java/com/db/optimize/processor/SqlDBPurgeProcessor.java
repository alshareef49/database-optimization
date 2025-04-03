package com.db.optimize.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
@Slf4j
public class SqlDBPurgeProcessor {

    @Value("${mysql.table.name}")
    private static String tableName;

    @Value("${mysql.database.name}")
    private static String databaseName;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final String GET_PARTITION_QUERY =
            "SELECT PARTITION_NAME FROM information_schema.PARTITIONS "
            + "WHERE TABLE_SCHEMA = " + databaseName +" AND TABLE_NAME = "
            + tableName + " AND PARTITION_NAME IS NOT NULL";


    @Scheduled(cron = "0 00 00 * * 7",zone = "America/chicago")
    public void sqlPurgeScheduler(){
        log.info("SQL Purge Scheduler Started.....");

        List<String> partitions = jdbcTemplate.queryForList(GET_PARTITION_QUERY,String.class);

        LocalDate cutoffDate = LocalDate.now().minusDays(3);
        partitionProcessor(partitions,cutoffDate);

        log.info("Purging Partition Completed");
    }

    public void partitionProcessor(List<String> partitions,LocalDate cutoffDate){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        for(String partitionName:partitions){
            if(partitionName.matches("\\d{8}")){ // Ensure format yyyyMMdd
                try{
                    LocalDate partitionDate = LocalDate.parse(partitionName,formatter);
                    if(partitionDate.isBefore(cutoffDate)){
                        dropPartition(partitionName);
                    }
                }catch (Exception e){
                    log.error("Exception Occurred while dropping the partition",e);
                }
            }else {
                log.info("Ignoring partition: {}",partitionName);
            }
        }
    }

    public void dropPartition(String partitionName){
        String dropPartitionSQL = "ALTER TABLE "+tableName+" DROP PARTITION `"+partitionName+"`";
        try{
            jdbcTemplate.execute(dropPartitionSQL);
            log.info("Dropped partition:{}",partitionName);
        }catch (DataAccessException e){
            log.error("Failed to delete Partition",e);
        }
    }



}
