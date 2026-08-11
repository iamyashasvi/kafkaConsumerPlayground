package com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Config;

import com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Model.Order;
import com.event_driven_architecture.kafkaConsumerPlayground.Consumer.Model.Payment;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConsumerConfigs {

    @Bean
    public ConsumerFactory<String, Order> orderConsumerFactory(KafkaProperties props){
        Map<String, Object> config = props.buildConsumerProperties();
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Order.class.getName());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConsumerFactory<String, Payment> paymentConsumerFactory(KafkaProperties props){
        Map<String, Object> config = props.buildConsumerProperties();
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Payment.class.getName());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenereFactory(ConsumerFactory<String, Order> orderConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Payment> paymentKafkaListenerFactory(ConsumerFactory<String, Payment> paymentConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<String, Payment> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentConsumerFactory);
        return factory;
    }
}
