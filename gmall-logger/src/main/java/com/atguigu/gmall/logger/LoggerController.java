package com.atguigu.gmall.logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.atguigu.gmall.common.Constant;

@RestController
public class LoggerController {
    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("/log")
    public String doLogger(@RequestParam String log){
        //给日志加时间戳
        log = addTS(log);
        //日志落盘
        saveToDisk(log);
        //实时发送数据到kafka
        sentToKafka(log);



        return "ok";
    }

    @Autowired
    private KafkaTemplate<String, String> kafka;

    /**
     * 发送给kafka
     * @param log
     */
    private void sentToKafka(String log) {
        if (log.contains ("startup")){
            kafka.send (Constant.STARTUP_TOPIC, log);
        } else {
            kafka.send (Constant.EVENT_TOPIC,log);
        }

    }

    /**
     * 日志落盘
     * 保留开发
     * @param log
     */
    private void saveToDisk(String log) {
        //记录info级别的日志
        logger.info (log);
    }

    private String addTS(String log) {
        JSONObject jsonObj = JSON.parseObject (log);
        jsonObj.put ("ts", System.currentTimeMillis ());
        return jsonObj.toJSONString ();
    }


}
