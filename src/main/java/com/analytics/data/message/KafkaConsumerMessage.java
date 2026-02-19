package com.analytics.data.message;


import com.analytics.data.dto.CarPostDTO;
import com.analytics.data.service.PostAnalyticsService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.sql.SQLOutput;

@Component
public class KafkaConsumerMessage {

    private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerMessage.class);

    @Autowired
    private PostAnalyticsService postAnalyticsService;

    @KafkaListener(topics = "car-post-topic", groupId = "analytics-posts-group")
    public void listening(@Payload CarPostDTO carPost) {

        System.out.println("Received Car Post Message: " + carPost);
        LOG.info("ANALYTICS DATA - Received Car Post information: {}", carPost);
        postAnalyticsService.saveDataAnalytics(carPost);
    }
}
