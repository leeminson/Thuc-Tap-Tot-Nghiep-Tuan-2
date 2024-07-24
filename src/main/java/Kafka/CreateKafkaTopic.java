/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Kafka;

/**
 *
 * @author pc
 */
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.BasicConfigurator;

public class CreateKafkaTopic {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        // Set properties for the admin client
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Create the admin client
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Define the topic
            String topicName = "my_topic11";
            int numPartitions = 1;
            short replicationFactor = 1;
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            // Create the topic
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

            System.out.printf("Topic '%s' created successfully%n", topicName);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

