package com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Listener;


import com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Model.Order;
import com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Model.Payment;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Consumer;

@Component
public class EventListener {

    @Autowired
    ObjectMapper objectMapper;

//    @KafkaListener(topics="order-events")
    public void consumer(Order order){
        //business logics
        System.out.println("Recieved order-events " + order.getOrderId());
    }

    /*
        Manul mapping
     */
    @KafkaListener(topics = {"order-events", "payment-events"})
    public void consumerManullMapping(ConsumerRecord<String, String> record) {
        if("order-events".equals(record.topic())){
            Order order = objectMapper.readValue(record.value(), Order.class);
            System.out.println("Order event consumed " + order.getOrderId());
        } else if("payment-events".equals((record.topic()))){
            Payment payment = objectMapper.readValue(record.value(), Payment.class);
            System.out.println("Payment event consumed " + payment.getPaymentId());
        }
    }

}
