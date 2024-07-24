/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.BasicConfigurator;

/**
 *
 * @author pc
 */
public class KafkaProduceMsg {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        System.out.println("start");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String>producer=new KafkaProducer<>(props);
        String topic="my_topic";
        String key="key";
        String value="testing Kafka";
        ProducerRecord<String,String>record=new ProducerRecord<>(topic,key,value);
        try{
            RecordMetadata metadata=producer.send(record).get();
            System.out.printf("Record is sent with key=%s value=%s to partition=%d offset=%d%n",
                    key, value, metadata.partition(), metadata.offset());
        }
        catch(Exception e){
            System.out.println(e);
        }
        finally {
            // Close the producer
            producer.close();
        }
    }
}
