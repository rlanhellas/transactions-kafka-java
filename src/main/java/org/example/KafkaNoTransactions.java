package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class KafkaNoTransactions {
    private static final String BOOTSTRAP_SERVERS = "192.168.1.106:32768";
    private static final String TOPICO_A = "topico-a";
    private static final String TOPICO_B = "topico-b";
    private static final String GROUP_ID = "consumer001-no-transaction";
    private static KafkaProducer<String, String> producer;

    private static int totalRecords = 0;
    private static final Logger logger = LoggerFactory.getLogger(KafkaNoTransactions.class);

    public static void main(String[] args) {
        long init = System.currentTimeMillis();

        producer = createKafkaProducer();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        consumer.subscribe(Collections.singletonList(TOPICO_A));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        do{

            totalRecords += records.count();
            logger.info("Qtd={}", records.count());
            for (ConsumerRecord<String, String> record : records) {
                producer.send(producerRecord(TOPICO_B, record));
            }
            consumer.commitSync(Duration.ofMinutes(5));

            records = consumer.poll(Duration.ofSeconds(1));
        }while (!records.isEmpty());

        long end = System.currentTimeMillis();
        logger.info("Total tempo={}, Total Records={}", (end - init), totalRecords);
    }

    private static ProducerRecord<String, String> producerRecord(String topicoB, ConsumerRecord<String, String> record) {
        return new ProducerRecord<>(topicoB, record.value().toString().concat("-->002-NO-TRANSACTION"));
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Map<String, Object> propsConsumer = new HashMap<>();
        propsConsumer.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        propsConsumer.put("group.id", GROUP_ID);
        propsConsumer.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(propsConsumer);
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Map<String, Object> propsProducer = new HashMap<>();
        propsProducer.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(propsProducer);
    }
}
