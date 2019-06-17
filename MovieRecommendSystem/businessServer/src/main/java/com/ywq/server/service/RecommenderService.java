package com.ywq.server.service;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.ywq.server.model.recom.Recommendation;
import com.ywq.server.model.request.GetHybirdRecommendationRequest;
import com.ywq.server.model.request.GetStreamRecsRequest;
import com.ywq.server.utils.Constant;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.List;

//用于推荐服务

@Service
public class RecommenderService {

    @Autowired
    private MongoClient mongoClient;

    /**
     * @param request
     * @return
     */
    public List<Recommendation> getHybirdRecommendations(GetHybirdRecommendationRequest request) {
        //获得实时推荐结果
        List<Recommendation> streamRecs=getStreamRecsMovies(new GetStreamRecsRequest(request.getUid(),request.getNum()));

        //获得ALS离线推荐结果

        //获得基于内容的推荐结果

        //返回结果

        return null;

    }

    /**
     * 获取当前用户的实时推荐
     * @param request
     * @return
     */
    public List<Recommendation> getStreamRecsMovies(GetStreamRecsRequest request) {
        MongoCollection<Document> streamRecsCollection = mongoClient.getDatabase(Constant.MONGO_DATABASE).getCollection(Constant.MONGO_STREAN_RECS_COLLECTION);
        Document document=streamRecsCollection.find(new Document("uid", request.getUid())).first();
        List<Recommendation> result = new ArrayList<>();
        if(null==document||document.isEmpty()){
            return result;
        }
        for(String item:document.getString("recs").split("\\|")){
            String[]  para=item.split(":");
            result.add(new Recommendation(Integer.parseInt(para[0]),Double.parseDouble(para[1])));
        }
        return result.subList(0,result.size()>request.getNum()?request.getNum():result.size());
    }
}
