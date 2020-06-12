package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    Long getDau(String date);


    Map<String, Long> getHourDau(String date);


    // 获取总的销售额
    Double getTotalAmount(String date);

    // 获取每小时的销售额
    // Map("10"->1000.1, "11" -> 2000,...)
    Map<String, Double> getHourAmount(String date);
}
