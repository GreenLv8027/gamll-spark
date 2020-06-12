package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

// 将来: 接口的具体实现, 有mybatis自动完成
public interface DauMapper {
    Long getDau(String date);

    // 定义获取指定日期的小时明细.
    List<Map<String, Object>> getHourDau(String date);
}
