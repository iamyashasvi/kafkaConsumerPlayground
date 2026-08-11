package com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Listener;


import com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Model.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    @KafkaListener(topics="order-events")
    public void consumer(Order order){
        //business logics
        System.out.println("Recieved order-events " + order.getOrderId());
    }

}
