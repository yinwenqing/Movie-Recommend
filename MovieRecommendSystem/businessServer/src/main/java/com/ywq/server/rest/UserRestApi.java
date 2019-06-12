package com.ywq.server.rest;

import com.ywq.server.model.core.User;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

//用于处理user相关的动作
@Controller
@RequestMapping("/test/users")
public class UserRestApi {

    //需要提供用户注册功能 /rest/users/register
    @RequestMapping(path="/register",produces = "application/json", method= RequestMethod.GET)
    @ResponseBody
    public User registerUser(String username, String password, Model model){
        User user=new User();
        user.setUsername("ll");
        user.setPassword("ds");
        user.getGenres().add("abd");
        user.getGenres().add("adsd");

        model.addAttribute("success",false);
        model.addAttribute("user",user);

        return null;
    }

    //需要提供用户登录功能
    @RequestMapping(path="/login",produces = "application/json", method= RequestMethod.GET)
    @ResponseBody
    public List<User> loginUser(String username, String password,Model model){
        User user=new User();
        user.setUsername("ll");
        user.setPassword("ds");
        user.getGenres().add("abd");
        user.getGenres().add("adsd");

    List<User> users=new ArrayList<User>();
        users.add(user);
        return users;
    }

    //需要能够添加用户偏爱的影片类别
    public Model addGeners(String username, String password,Model model){
        return null;
    }
}
