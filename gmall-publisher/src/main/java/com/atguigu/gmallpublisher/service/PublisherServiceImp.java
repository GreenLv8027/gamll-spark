package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dau;

    @Override
    public Long getDau(String date) {
        return dau.getDau (date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dau.getHourDau(date);
        Map<String, Long> result = new HashMap<> ();
        for (Map<String, Object> map : hourDauList) {
            String key = (String) map.get("LOGHOUR");
            Long value = (Long) map.get("COUNT");
            result.put(key, value);
        }
        return result;
    }

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Double getTotalAmount(String date) {
        // 当指定日期中没有数据的时候, 则这个地方会得到null
        Double totalAmount = orderMapper.getTotalAmount(date);
        return totalAmount == null ? 0 : totalAmount;
    }

    @Override
    public Map <String, Double> getHourAmount(String date) {
        // Map("create_hour"->"10", "sum" -> 1000)  => "10"->1000
        List<Map<String, Object>> hourAmountList = orderMapper.getHourAmount(date);
        Map<String, Double> result = new HashMap<>();
        for (Map<String, Object> map : hourAmountList) {
            String key = (String) map.get("CREATE_HOUR");
            Double value = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(key, value);
        }

        return result;
    }
}
