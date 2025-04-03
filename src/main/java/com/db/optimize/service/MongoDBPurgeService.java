package com.db.optimize.service;

import com.db.optimize.models.ExpireDoc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class MongoDBPurgeService {

    @Autowired
    private MongoService mongoService;
    static final ObjectMapper objectMapper = new ObjectMapper();
    static final JsonWriterSettings settings = JsonWriterSettings.builder()
            .dateTimeConverter((value, writer) -> {
                writer.writeNumber(value.toString());
            }).build();

    public void deleteDocsProcessor(String collection) {
        ZonedDateTime currentDateTime = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime dateTime3daysBack = currentDateTime.minusDays(3);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime3daysBack.toString().substring(0, 30), formatter);
        Instant instant = Instant.parse(localDateTime.toString() + "Z");
        long epochMilliseconds = instant.toEpochMilli();

        List<ExpireDoc> expireDocList = new ArrayList<>();
        MongoCollection<Document> documentMongoCollection = mongoService.getCollection(collection);

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.ne("_id", null)),
                Aggregates.project(Projections.fields(
                        Projections.include("_id", "expirationDtTm"),
                        Projections.computed("expirationDtTm", new Document("$toDate", "$expirationDtTm"))
                ))
        );

        int currentPos = 0;
        try (MongoCursor<Document> cursor = documentMongoCollection.aggregate(pipeline).batchSize(10).iterator()) {
            while (cursor.hasNext()) {
                if (currentPos == 1000) {
                    deleteDoc(expireDocList,epochMilliseconds,documentMongoCollection,collection);
                }
                Document document = cursor.next();
                ExpireDoc expireDoc = convertDoc(document, ExpireDoc.class);
                currentPos++;
                expireDocList.add(expireDoc);
            }
        } catch (Exception e) {
            log.error("Exception occurred", e.getCause());
        }
    }

    private void deleteDoc(List<ExpireDoc> expireDocList, long epochMilliseconds, MongoCollection<Document> documentMongoCollection, String collection) {
        for(ExpireDoc doc:expireDocList){
            if(doc.getExpireDtTm()!=null && Long.parseLong(doc.getExpireDtTm()) < epochMilliseconds){
                mongoService.deleteById(collection,doc.get_id());
            }
        }
    }

    private <T> T convertDoc(Document document, Class<T> clazz) {
        if (document != null) {
            try {
                return objectMapper.readValue(document.toJson(settings), clazz);
            } catch (IOException e) {
                log.error("Exception Occurred", e.getCause());
            }
        }
        return null;
    }


}
