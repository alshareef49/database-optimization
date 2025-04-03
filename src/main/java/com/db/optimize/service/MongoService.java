package com.db.optimize.service;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
public class MongoService {

    private final MongoTemplate mongoTemplate;

    public MongoService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public MongoCollection<Document> getCollection(String collectionName) {
        return mongoTemplate.getCollection(collectionName);
    }

    public void deleteById(String collectionName, String id) {
        Query query = new Query(Criteria.where("_id").is(new ObjectId(id))); // Ensure it's an ObjectId
        mongoTemplate.remove(query, collectionName);
    }
}
