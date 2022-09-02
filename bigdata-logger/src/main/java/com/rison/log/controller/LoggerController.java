package com.rison.log.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @PACKAGE_NAME: com.rison.log.controller
 * @NAME: LoggerController
 * @USER: Rison
 * @DATE: 2022/9/2 10:10
 * @PROJECT_NAME: flink-realtime-demo
 **/

@RestController
@Slf4j
public class LoggerController {

    @Value("${KAFKA-USER}")


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {

        log.info(jsonStr);

        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
