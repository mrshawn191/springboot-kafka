package com.example.demo.consumer;

import com.example.demo.model.Route;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

public class LogConsumer extends AbstractLogConsumer
{

    // redis client library
    private JedisPool jedisPool;

    public static void main(String[] args) throws URISyntaxException
    {
        new LogConsumer().start();
    }

    public LogConsumer() throws URISyntaxException
    {
        if (System.getenv("REDIS_URL") == null)
        {
            throw new IllegalArgumentException("No REDIS_URL is set");
        }

        URI redisUri = new URI(System.getenv("REDIS_URL"));

        jedisPool = new JedisPool(redisUri);
    }

    @Override
    public void stopConsuming()
    {
        jedisPool.destroy();
        super.stopConsuming();
    }

    @Override
    public void receiveRoute(Route route)
    {
        String path = route.getMessageKey("path");

        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            byte[] hash = messageDigest.digest(path.getBytes(StandardCharsets.UTF_8));
            String pathDigest = new String(hash);

            // establishing redis connection
            try (Jedis jedis = jedisPool.getResource())
            {
                jedis.hset("routes", path, pathDigest);

                // Heroku logs format example
                // 2017-05-11T10:50:26.716426+00:00 heroku[router]: at=info method=GET path="/static/css/main.b4ace3a9.css"
                // host=frozen-beyond-34446.herokuapp.com request_id=80416e6b-f910-44ab-a9f9-6eefdc4dcbb6 fwd="83.227.72.30" dyno=web.1 connect=0ms service=4ms status=200 bytes=571 protocol=https
                for (String metric : Arrays.asList("service", "connect"))
                {
                    Integer valueOfMilliseconds = Integer.valueOf(route.getMessageKey(metric).replace("ms", ""));
                    String key = pathDigest + "::" + metric;

                    jedis.hincrBy(key, "sum", valueOfMilliseconds);
                    jedis.hincrBy(key, "count", 1);

                    Integer sum = Integer.valueOf(jedis.hget(key, "sum"));
                    Float count = Float.valueOf(jedis.hget(key, "count"));
                    Float average = sum / count;

                    jedis.hset(key, "average", String.valueOf(average));
                }
                jedis.hincrBy(pathDigest + "::statuses", route.getMessageKey("status"), 1);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
