package com.zhoubc.mdata.sync.es;

import com.zhoubc.mdata.sync.utils.MapUtil;

import com.zhoubc.mdata.sync.utils.Validate;
import com.zhoubc.mdata.sync.utils.Pair;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 18:18
 */
public class ElasticSearchClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientFactory.class);
    private static final RequestOptions REQUEST_OPTIONS;

    //endpoint就是 "ip:port"
    //key=endpoint,value=[username,RestHighLevelClient]; 同一个endpoint只能一个实例，
    private static final Map<String, Pair<String,RestHighLevelClient>> CLENT_MAP = MapUtil.concurrentHashMap();

    static {
        int bufferLimitBytes = 30 * 1024 * 1024; //默认缓存限制100MB，此处修改为30MB
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(bufferLimitBytes));
        REQUEST_OPTIONS = builder.build();
    }

    public static RequestOptions getRequestOptions() {// 写入es时会用到
        return REQUEST_OPTIONS;
    }

    public static RestHighLevelClient getClient(String endpoint){
        Pair<String,RestHighLevelClient> p = CLENT_MAP.get(endpoint);
        Validate.assertNotNull(p,"无法找到es的restHighLevelClient:"+endpoint);
        RestHighLevelClient client = p.getRight();
        return  client;
    }

    public static void registerRestHighLevelClient(String endpoint, String userName, String password, Map<String,Object> properties){
        Validate.assertNotEmpty(endpoint,"endpoint不能为空");
//        Validate.assertNotEmpty(userName,"userName不能为空");
//        Validate.assertNotEmpty(password,"password不能为空");

        if(CLENT_MAP.containsKey(endpoint)){
            Pair<String,RestHighLevelClient> p = CLENT_MAP.get(endpoint);
            if(p != null && p.getRight() != null){
                if(p.getLeft().equals(userName)){
                    logger.warn("restHighLevelClient已存在：endpoint={},userName={}", endpoint, userName);
                    return;
                }else{
                    throw new RuntimeException("restHighLevelClient已存在：endpoint="+endpoint+"，但userName校验不通过，已有"+p.getLeft()+"，传入"+userName);
                }
            }
        }

        //从CLENT_MAp获取restHighLevelClient，获取不到就创建新的客户端并放到Map里
        MapUtil.getValue(CLENT_MAP, endpoint, (String dsUniqueKey) -> {
            RestHighLevelClient client = createRestHighLevelClient(endpoint, userName, password, properties);
            return Pair.of(userName, client);
        });

        logger.info("===》成功创建了restHighLevelClient：endpoint={},userName={}", endpoint, userName);

    }

    public static RestHighLevelClient createRestHighLevelClient(String endpoint, String userName, String password, Map<String,Object> properties){
        Validate.assertNotEmpty(endpoint,"endpoint不能为空");
//        Validate.assertNotEmpty(userName,"userName不能为空");
//        Validate.assertNotEmpty(password,"password不能为空");

        String endpointArray[] = endpoint.split(":");
        String host = endpointArray[0];
        int port = endpointArray.length == 2 ? Integer.valueOf(endpointArray[1]) : 9200;

        RestClientBuilder builder = RestClient.builder(new HttpHost(host,port));
        builder.setRequestConfigCallback(requestConfigBuilder->{
            requestConfigBuilder.setConnectTimeout(5000);
            requestConfigBuilder.setSocketTimeout(5000);
            requestConfigBuilder.setConnectionRequestTimeout(1000);
            return requestConfigBuilder;
        });

        int maxConnectionBumber = MapUtil.getInt(properties, "maxConnectionBumber", 16);
        // 阿里云Elasticsearch集群需要basic auth验证
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        // 访问用户名和密码为您创建阿里云Elasticsearch实例时设置的用户名和密码，也是Kibana控制台的登录用户和密码
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,password));

        builder.setHttpClientConfigCallback(clientConfigBuilder->{
            clientConfigBuilder.setMaxConnTotal(maxConnectionBumber);
            clientConfigBuilder.setMaxConnPerRoute(200);
            //clientConfigBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return clientConfigBuilder;
        });
        return new RestHighLevelClient(builder);
    }


}
