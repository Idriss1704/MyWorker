package de.fernunihagen.lenneflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {

    private final static String QUEUE1_NAME = "TypeA";
    private final static String QUEUE2_NAME = "TypeB";
    private final static String RESULT_QUEUE_NAME = "taskResultQueue";

    public static void main(String[] args) throws IOException, TimeoutException {
        BasicConfigurator.configure();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.70.129");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        factory.setUsername("rabbit");
        factory.setPassword("rabbit");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE1_NAME, true, false, false, null);
        channel.queueDeclare(QUEUE2_NAME, true, false, false, null);
        channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            ObjectMapper mapper = new ObjectMapper();
            Task task = mapper.readValue(delivery.getBody(), Task.class);
            System.out.println(" [x] Received '" + task.getTaskName() + "'");
            try {
                doWork(task);
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(QUEUE1_NAME, false, deliverCallback, consumerTag -> { });
    }

    private static void doWork(Task task) {
      TaskResult result = new TaskResult();
      result.setMetaData(task.getMetaData());
      System.out.println("Processing task: " + task.getMetaData());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}