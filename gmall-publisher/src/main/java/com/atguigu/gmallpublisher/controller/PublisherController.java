package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService service;

    // http://localhost:8070/realtime-total?date=2020-05-29
    @GetMapping("/realtime-total")
    public String realTotal(@RequestParam("date") String date) {
        //用List存储每个Map，结果转为Json类型返回
        ArrayList <Map<String, String>> result = new ArrayList <> ();

        Map<String, String> map1 = new HashMap <> ();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", service.getDau(date).toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", service.getTotalAmount(date).toString());
        result.add(map3);

        return JSON.toJSONString(result);
    }


}
