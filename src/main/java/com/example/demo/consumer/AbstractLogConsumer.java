package com.example.demo.consumer;

import com.example.demo.config.KafkaConfig;
import com.example.demo.model.Route;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;

public abstract class AbstractLogConsumer
{

    public abstract void receiveRoute(Route route);

    private final AtomicBoolean isRunning = new AtomicBoolean();

    private ExecutorService executorService;

    private CountDownLatch countDownLatch;

    private ObjectMapper objectMapper = new ObjectMapper();

    // requires you to specify parameters statically
    private TypeReference<HashMap<String, Object>> typeReference = new TypeReference<HashMap<String, Object>>()
    {

    };

    private KafkaConsumer<String, String> kafkaConsumer;

    public void start()
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopConsuming()));

        isRunning.set(true);
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::startConsuming);
        countDownLatch = new CountDownLatch(1);
    }

    public void stopConsuming()
    {
        isRunning.set(false);
        try
        {
            // will wait until count reaches zero or is interrupted by another thread.
            countDownLatch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            executorService.shutdown();
        }
    }

    /**
     * Starts consuming from the queue
     */
    private void startConsuming()
    {
        // TODO: add logging
        Properties properties = new Properties();
        properties.putAll(KafkaConfig.getConsumerProperties());

        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(singletonList(KafkaConfig.getLogTopic()));

        while (isRunning.get())
        {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
            {
                try
                {
                    Map<String, String> recordMap = objectMapper.readValue(consumerRecord.value(), typeReference);
                    Route route = new Route(consumerRecord);
                    receiveRoute(route);
                }
                catch (IOException e)
                {
                    // TODO: add logging
                    e.printStackTrace();
                }
            }
        }

        // closing consumer
        kafkaConsumer.close();
        countDownLatch.countDown();
    }
}
