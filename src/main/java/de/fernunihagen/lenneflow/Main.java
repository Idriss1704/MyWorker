package de.fernunihagen.lenneflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
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
        factory.setHost("UbuntuDev");
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
            System.out.println(" [x] Received '" + task.getTaskStatus() + "'");
            try {
               Task result = doWork(task);
               byte[] serializedTaskResult = mapper.writeValueAsBytes(result);
               channel.basicPublish("", RESULT_QUEUE_NAME, null, serializedTaskResult);
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(QUEUE1_NAME, false, deliverCallback, consumerTag -> { });
        channel.basicConsume(QUEUE2_NAME, false, deliverCallback, consumerTag -> { });
    }

    private static Task doWork(Task task) {
      Task result = new Task();
      result.setMetaData(task.getMetaData());
      result.setRunNode("WORKER");
      System.out.println("Processing task: " + result.getMetaData());
        try {
            result.setTaskStatus(TaskStatus.COMPLETED);
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}