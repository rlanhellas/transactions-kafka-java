package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class KafkaJustProduce {
    private static final String BOOTSTRAP_SERVERS = "192.168.1.106:32768";
    private static final String TOPICO_A = "topico-a";

    private static KafkaProducer<String, String> producer;
    private static final int QTD_REGISTROS = 100000;

    public static void main(String[] args) {
        producer = createKafkaProducer();

        for (int i = 0; i < QTD_REGISTROS; i++) {
            producer.send(new ProducerRecord<>(TOPICO_A, "001"));
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Map<String, Object> propsProducer = new HashMap<>();
        propsProducer.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        propsProducer.put(ProducerConfig.ACKS_CONFIG, "-1");
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(propsProducer);
    }
}
