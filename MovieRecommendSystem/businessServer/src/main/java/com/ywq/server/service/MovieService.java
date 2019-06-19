package com.ywq.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;
import com.ywq.server.model.core.Movie;
import com.ywq.server.model.recom.Recommendation;
import com.ywq.server.utils.Constant;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class MovieService {

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private ObjectMapper objectMapper;

    private MongoCollection<Document> movieCollection;

    private MongoCollection<Document> getMongoCollection(){
        if(null == movieCollection){
            this.movieCollection=mongoClient.getDatabase(Constant.MONGO_DATABASE).getCollection(Constant.MONGO_MOVIE_COLLECTION);
        }
        return this.movieCollection;
    }

    private Movie documentToMovie(Document document){
        try {
            Movie movie = objectMapper.readValue(JSON.serialize(document), Movie.class);
            return movie;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Movie> getMoviesByMids(List<Integer> ids){
        FindIterable<Document> documents=getMongoCollection().find(Filters.in("mid", ids));



    }

    public
}
