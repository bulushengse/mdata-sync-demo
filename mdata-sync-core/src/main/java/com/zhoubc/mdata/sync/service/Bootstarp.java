package com.zhoubc.mdata.sync.service;

import com.alibaba.fastjson.JSONObject;
import com.zhoubc.mdata.sync.es.ElasticSearchClientFactory;
import com.zhoubc.mdata.sync.kafka.KafkaConsumerProperties;
import com.zhoubc.mdata.sync.kafka.KafkaListenner;
import com.zhoubc.mdata.sync.tbuilder.Environment;
import com.zhoubc.mdata.sync.tconfig.OutputConfig;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/5 18:27
 */
public class Bootstarp{

    public void init(){

        System.out.println("init-------------------------------------------------------------start");

        // 创建ES的RestHighLevelClient
        initElasticSearchClients();

        // 注册ES索引配置，放入缓存MAP
        initDataSources();

        // Kafka开始监听，binlog变动触发ES更新
        initKafkaTipics();

        System.out.println("init-------------------------------------------------------------end");
    }

    private void initElasticSearchClients(){
        ElasticSearchClientFactory.registerRestHighLevelClient("127.0.0.1:9200","","",null);
    }

    private void initKafkaTipics(){
        KafkaConsumerProperties properties = new KafkaConsumerProperties();
        //properties.setUsername("");
        //properties.setPassword("");
        properties.setGroup("test-kafkaConsumer-group");
        properties.setTopic("zhoubc_test");
        properties.setUrl("127.0.0.1:9092");

        KafkaListenner.start(properties);
    }

    private void initDataSources(){
        //获取resources/config目录下的数据库表与索引表的表关联配置文件
        List<OutputConfig> dlist = getConfigsResource();
        //配置里的数据源信息 放入缓存
        for (OutputConfig item : dlist) {
            Environment.registerSchema(item);
        }
    }

    private List<OutputConfig> getConfigsResource() {
        List<String> files = readConfigsResourceFileInfo();
        List<OutputConfig> result = new ArrayList<>();
        for (String content : files) {
            OutputConfig cf = JSONObject.parseObject(content, OutputConfig.class);
            //String destination = cf.getDestination();//索引名称
            //校验配置文件是否正确



            result.add(cf);
        }

        return result;
    }

    private List<String> readConfigsResourceFileInfo() {
        PathMatchingResourcePatternResolver r = new PathMatchingResourcePatternResolver();
        Resource[] resources = null;
        try {
            resources = r.getResources("classpath*:/config/*.json");
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(resources == null){
            return null;
        } else {
            List<String> list = new ArrayList(resources.length);
            for (Resource Resource : resources) {
                try {
                    String str = fileToString(Resource.getInputStream(), "utf-8");
                    if (str != null) {
                        list.add(str);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return list;
        }
    }

     private String fileToString(InputStream inputStream, String charset) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];

        int length;
        while((length = inputStream.read(buffer)) != -1){
            result.write(buffer, 0, length);
        }

        return result.toString(charset);
    }

    private void test(){
        RestHighLevelClient client = ElasticSearchClientFactory.getClient("127.0.0.1:9200");

        SearchRequest searchRequest = new SearchRequest("springboot-2020.12.18");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(10);
        searchSourceBuilder.from(1);
        searchRequest.source(searchSourceBuilder);

        SearchHit[] hitArray= new SearchHit[0];
        try {
            hitArray = client.search(searchRequest, RequestOptions.DEFAULT).getHits().getHits();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (SearchHit hit:hitArray) {
            System.out.println("111111111111111111111"+hit.toString());
        }
    }



}
