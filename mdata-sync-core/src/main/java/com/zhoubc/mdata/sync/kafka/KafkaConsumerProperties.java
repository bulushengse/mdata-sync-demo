package com.zhoubc.mdata.sync.kafka;

import lombok.Data;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 20:45
 */
@Data
public class KafkaConsumerProperties {

    private String topic;

    private String url;

    private String username;

    private String password;

    private String group;


}
