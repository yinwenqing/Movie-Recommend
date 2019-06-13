package com.ywq.server.rest;

import com.ywq.server.model.core.User;
import com.ywq.server.model.request.RegisterUserRequest;
import com.ywq.server.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

//用于处理user相关的动作
@Controller
@RequestMapping("/test/users")
public class UserRestApi {

    @Autowired
    private UserService userService;

    /**
     * 需要提供用户注册功能
     * url: /rest/users/register?username=abc&password=abc
     * @param username
     * @param password
     * @param model
     * @return
     */
    @RequestMapping(path="/register",produces = "application/json", method= RequestMethod.GET)
    @ResponseBody
    public User registerUser(@RequestParam("username") String username, @RequestParam("password") String password, Model model){

        model.addAttribute("success",userService.registerUser(new RegisterUserRequest(username,password)));
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
