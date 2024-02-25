package com.zhoubc.mdata.sync.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/5 19:39
 */
public class KafkaHelper {

    //KafkaConsumer
    //private static final Map<String,KafkaConsumer<byte[],byte[]>> CONSUMER_MAP = new ConcurrentHashMap(32);


    public static KafkaConsumer<byte[],byte[]> buildConsumer(KafkaConsumerProperties props, int maxPollSize){
        Properties p = buildConsumerProperties(props, maxPollSize);
        KafkaConsumer<byte[],byte[]> KafkaConsumer = new KafkaConsumer(p);
        //CONSUMER_MAP.put(props.getTopic()+"."+ props.getGroup(), KafkaConsumer);
        return  KafkaConsumer;
    }


    private static Properties buildConsumerProperties(KafkaConsumerProperties props, int maxPollSize) {
        Properties p = new Properties();
        //p.put(SaslConfigs.SASL_JAAS_CONFIG,
        //        buildJaasConfig(props.getGroup(), props.getUsername(), props.getPassword()));
        //p.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        //p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getUrl());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, props.getGroup());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//禁用自动提交
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //p.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, );
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollSize);//kafkaConsumer.poll方法每次拉取最多消息数量

        //p.put(ConsumerConfig.CLIENT_ID_CONFIG,"");//自定义client_id
        return p;
    }


    private static String buildJaasConfig(String sid, String user, String password){
        String s = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s-%s\" password=\"%s\";";
        return String.format(s, user, sid, password);
    }




}
