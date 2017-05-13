package com.example.demo.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.http.HttpStatus;
import org.springframework.integration.syslog.inbound.RFC6587SyslogDeserializer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@RestController
public class LogsController implements BeanFactoryAware
{

    private BeanFactory context;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException
    {
        this.context = beanFactory;
    }

    @RequestMapping(value = "/api/logs/health", method = RequestMethod.GET)
    @ResponseStatus(value = HttpStatus.OK)
    public void healthCheck()
    {
    }

    @RequestMapping(value = "/api/logs/create", method = RequestMethod.POST)
    public String logs(@RequestBody String body) throws IOException
    {
        // "application/logplex-1" does not conform to RFC5424.
        // It leaves out STRUCTURED-DATA but does not replace it with
        // a NILVALUE. To workaround this, we inject empty STRUCTURED-DATA.
        String[] parts = body.split("router - ");
        String log = parts[0] + "router - [] " + (parts.length > 1 ? parts[1] : "");

        // Knows how to speak the language of syslog => turn syslog => into a map => into a json => into a string
        RFC6587SyslogDeserializer parser = new RFC6587SyslogDeserializer();
        InputStream is = new ByteArrayInputStream(log.getBytes());
        Map<String, ?> messages = parser.deserialize(is);

        // Parse json to string
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(messages);

        // Get Producer through dependency injection
        MessageChannel toKafka = context.getBean("toKafka", MessageChannel.class);

        // Sends message through kafka producer
        // Send is not async, kafka producer in a background thread will queue up each message and send into the broker in batches
        // Send them in batches of the type of message that is sent
        toKafka.send(new GenericMessage<>(json));

        return "ok";
    }

}
