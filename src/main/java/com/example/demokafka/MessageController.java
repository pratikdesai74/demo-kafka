package com.example.demokafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final String TOPIC="jbdl13-msg";

     private static Logger logger= LoggerFactory.getLogger(MessageController.class);

    @PostMapping("/message")
    public void produceMessage(@RequestParam("msg")String message){
        kafkaTemplate.send(TOPIC,message);
        logger.info("produced a message= {} -----> on a topic = {}",message,TOPIC);
    }

    @KafkaListener(topics = {TOPIC})
    public void consumeMessage(String myMessage){
        logger.info("Consumer Received produced message = {}",myMessage);
    }
}
