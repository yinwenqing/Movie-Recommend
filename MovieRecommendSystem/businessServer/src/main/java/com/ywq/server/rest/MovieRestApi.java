package com.ywq.server.rest;

import com.ywq.server.model.core.Movie;
import com.ywq.server.model.core.User;
import com.ywq.server.model.recom.Recommendation;
import com.ywq.server.model.request.GetStreamRecsRequest;
import com.ywq.server.service.RecommenderService;
import com.ywq.server.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

//用于处理Movie相关的功能
@Controller
@RequestMapping("/rest/movies")
public class MovieRestApi {

    @Autowired
    private RecommenderService recommenderService;

    @Autowired
    private UserService userService;

    //***********首页功能**********

    /**
     * 提供获取实时推荐信息的接口[混合推荐]
     * 访问：url: /rest/movies/stream?username=abc&num=10
     * 返回：{success:true,movies:[]}
     * @param username
     * @param sum
     * @param model
     * @return
     */
    @RequestMapping(path="/stream",produces="application/json",method= RequestMethod.GET)
    public Model getRealtimeRecommendations(@RequestParam("username") String username, @RequestParam("number") int sum, Model model){
        User user= userService.findUserByUsername(username);
        List<Recommendation> recommendations= recommenderService.getStreamRecsMovies(new GetStreamRecsRequest(user.getUid(),sum));
        List<Movie>
        model .addAttribute("success",true);
        return model;
    }

    //提供获取离线推荐信息的接口
    public Model getOfflineRecommender(String username, Model model){

        return null;
    }

    //提供获取热门推荐信息的接口
    public Model getHotRecommendations(Model model){

        return null;
    }

    //提供获取优质电影的接口
    public Model getRateMoreRecommendations(Model model){

        return null;
    }

    //获取最新电影的信息的接口
    public Model getNewRecommendations(Model model){

        return null;
    }


    //***********模糊检索***********

    //提供基于名称或者描述的模糊检索功能
    public Model getFuzzySearchMovies(String query,Model model){
        return null;
    }


    //***********电影的详细页面***********

    //获取单个电影的信息
    public Model getMovieInfo(int mid){
        return null;
    }

    //需要提供能够给电影打标签的功能
    public Model addTagToMovie(int mid,String tagname,Model model){
        return null;
    }

    //需要提供获取电影的所有标签信息
    public Model getMovieTags(int mid, Model model ){
        return null;
    }

    //需要能够获取电影相似的电影推荐
    public Model getSimMoviesRecommendation(int mid, Model model ){
        return null;
    }

    //需要能够提供给电影打分的功能
    public Model rateMovie(int mid, Double score,Model model){
        return null;
    }


    //***********电影的类别页面***********

    //需要能够提供影片类别的查找
    public Model getGenresMovies(String genres,Model model){
        return null;
    }


    //***********用户的空间页面***********

    //需要提供用户的所有电影评分记录
    public Model getUSerRatings(String username ,Model model){
        return null;
    }

    //需要能够获取图表数据
    public Model getUserChart(String username,Model model){
        return null;
    }


}
