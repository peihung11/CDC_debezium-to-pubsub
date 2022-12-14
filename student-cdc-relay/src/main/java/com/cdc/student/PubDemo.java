package com.cdc.student;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import io.debezium.config.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.springframework.stereotype.Component;


@Component
public class PubDemo  {
    // 初始化TopicName
    private TopicName topicName;
    private Publisher publisher;
 
    public PubDemo(Configuration studentConnector) {
        initPublisher(studentConnector);
    }
 
    // 初始化Publisher
    private void initPublisher(Configuration studentConnector) {
        try{

            Properties props = studentConnector.asProperties();
            
            String das_ct_lab = props.getProperty("project.id");
            String student_topic_test = props.getProperty("topic.id");

            topicName = TopicName.of(das_ct_lab, student_topic_test);

            // Create a publisher and set message ordering to true.
            publisher = Publisher.newBuilder(topicName)
                // Sending messages to the same region ensures they are received in order
                // even when multiple publishers are used.
                .setEndpoint("us-east1-pubsub.googleapis.com:443")
                .setEnableMessageOrdering(true)
                .build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    public void sendMessage(Map<String, String> messages) throws IOException, InterruptedException {
        
       
            
        for (Map.Entry<String, String> entry : messages.entrySet()) {
            ByteString data = ByteString.copyFromUtf8(entry.getKey());
            PubsubMessage pubsubMessage =
                PubsubMessage.newBuilder().setData(data).setOrderingKey(entry.getValue()).build();
            ApiFuture<String> future = publisher.publish(pubsubMessage);

            // Add an asynchronous callback to handle publish success / failure.
            ApiFutures.addCallback(
                future,
                new ApiFutureCallback<String>() {

                @Override
                public void onFailure(Throwable throwable) {
                    if (throwable instanceof ApiException) {
                    ApiException apiException = ((ApiException) throwable);
                    // Details on the API exception.
                    System.out.println(apiException.getStatusCode().getCode());
                    System.out.println(apiException.isRetryable());
                    }
                    System.out.println("Error publishing message : " + pubsubMessage.getData());
                }

                @Override
                public void onSuccess(String messageId) {
                    // Once published, returns server-assigned message ids (unique within the topic).
                    System.out.println(pubsubMessage.getData() + " : " + messageId);
                }
                },
                MoreExecutors.directExecutor());
        }
        
    }
}
    

