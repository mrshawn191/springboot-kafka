package com.example.demo.config;

import com.github.jkutner.EnvKeyStore;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConfig
{

    public static String getLogTopic()
    {
        return "logs";
    }

    public static String getLogMessageKey()
    {
        return "logs.key";
    }

    /**
     * Gets default producer properties
     */
    public static Map<String, Object> getProducerProperties()
    {
        Map<String, Object> properties = getDefaultKafkaProperties();
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // Sets how to serialize key
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Sets how to serialize value
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    /**
     * Gets default consumer properties
     */
    public static Map<String, Object> getConsumerProperties()
    {
        Map<String, Object> properties = getDefaultKafkaProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dev");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // Sets how to deserialize key
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Sets how to deserialize value
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    /**
     * Gets default kafka properties for both ssl and non-ssl
     */
    private static Map<String, Object> getDefaultKafkaProperties()
    {
        Map<String, Object> properties = new HashMap<>();
        List<String> hostPorts = new ArrayList<>();

        String kafkaUrl = getEnv("KAFKA_URL");
        for (String url : kafkaUrl.split(","))
        {
            try
            {
                URI uri = new URI(url);
                hostPorts.add(String.format("%s:%d", uri.getHost(), uri.getPort()));

                switch (uri.getScheme())
                {
                    case "kafka":
                        setKafkaProperties(properties);
                        break;
                    case "kafka+ssl":
                        try
                        {
                            setKafkaSSLProperties(properties);
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException("Failed to create kafka key stores", e);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("unknown scheme: %s", uri.getScheme()));
                }
            }
            catch (URISyntaxException e)
            {
                throw new RuntimeException(e);
            }
        }

        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", hostPorts));
        return properties;
    }

    /**
     * Sets non-ssl kafka properties
     */
    private static void setKafkaProperties(Map<String, Object> properties)
    {
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
    }

    /**
     * Sets ssl kafka properties
     */
    private static void setKafkaSSLProperties(Map<String, Object> properties) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException
    {
        EnvKeyStore envTrustStore = EnvKeyStore.createWithRandomPassword("KAFKA_TRUSTED_CLIENT");
        EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword("KAFKA_CLIENT_CERT_KEY", "KAFKA_CLIENT_CERT");

        properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, envTrustStore.storeTemp().getAbsolutePath());
        properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envKeyStore.password());
        properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
        properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, envKeyStore.storeTemp().getAbsolutePath());
        properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());
    }

    /**
     * Returns the environmental variable for the given url
     */
    private static String getEnv(String url)
    {
        String value = System.getenv(url);
        if (value == null)
        {
            throw new IllegalArgumentException("Environment variable is not set");
        }
        return value;
    }

}
