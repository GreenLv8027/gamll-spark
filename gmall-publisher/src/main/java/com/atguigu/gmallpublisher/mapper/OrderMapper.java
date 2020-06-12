package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
        // 得到指定日期的销售总额
        Double getTotalAmount(String date);


        List<Map<String, Object>> getHourAmount(String date);

}
