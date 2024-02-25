package com.zhoubc.mdata.sync.es;

import com.zhoubc.mdata.sync.utils.ExecutorUtil;
import com.zhoubc.mdata.sync.utils.Result;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo.Failure;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/3/19 20:56
 */
public class EsDocWriter {
    private static final Logger logger = LoggerFactory.getLogger(EsDocWriter.class);
    private static final int RETRY_TIMES = 2;

    private static Result<String> getResult(DocWriteRequest<?> request, DocWriteResponse response){
        if(response == null){
            return Result.err(500,"response为null，有异常");
        }

        ShardInfo si = response.getShardInfo();

        if (si == null) {
            return Result.err(500, "response.getShardInfo()为null");
        }

        if (si.getFailed() == 0) {
            return Result.ok(response.getId());
        }

        Failure[] arr = si.getFailures();
        if (arr.length == 0) {
            return Result.ok(response.getId());
        }

        return Result.of(500, response.getId(), arr[0].reason());
    }


    /**
     * 插入文档 如果ID已经存在，会覆盖老文档，index时会检查_version，如果插入时没有指定_verison,那对于已有的doc，_version会递增，并对文档覆盖，
     * 插入时如果指定_version，如果与已有的文档_version不相等，则插入失败，如果相等则覆盖，_version递增。
     *
     * @param request 请求
     * @param key  ES配置属性
     * @return
     */
    public static Result<String> indexDoc(IndexRequest request, String key){
        if (request == null) {
            return Result.err(0);
        }

        RestHighLevelClient restHighLevelClient = ElasticSearchClientFactory.getClient(key);

        IndexResponse response = ExecutorUtil.callWithRetry(() -> {
            return restHighLevelClient.index(request, ElasticSearchClientFactory.getRequestOptions());
        }, RETRY_TIMES, (i, e)-> {
            if (i == RETRY_TIMES - 1) {
                logger.error("index error, request={}, key={}, exception={}", request, key, e);
            }
            return true;
        });

        Result<String> r = getResult(request, response);
        return r;
    }

    /**
     * 修改文档，可以支持局部修改，但是性能较index差，因为会多查一次原始数据
     *
     * @param request 请求
     * @param key  ES配置属性
     * @return
     */
    public static Result<String> updateDoc(UpdateRequest request, String key){
        if (request == null) {
            return Result.err(0);
        }

        RestHighLevelClient restHighLevelClient = ElasticSearchClientFactory.getClient(key);

        UpdateResponse response = ExecutorUtil.callWithRetry(() -> {
            return restHighLevelClient.update(request, ElasticSearchClientFactory.getRequestOptions());
        }, RETRY_TIMES, (i, e)-> {
            if (i == RETRY_TIMES - 1) {
                logger.error("update error, request={}, key={}, exception={}", request, key, e);
            }
            return true;
        });

        Result<String> r = getResult(request, response);
        return r;
    }

    /**
     * 删除文档，根据id删除文档
     *
     * @param request 请求
     * @param key  ES配置属性
     * @return
     */
    public static Result<String> deleteDoc(DeleteRequest request, String key){
        if (request == null || StringUtils.isBlank(request.id())) {
            return Result.err(0);
        }

        RestHighLevelClient restHighLevelClient = ElasticSearchClientFactory.getClient(key);

        DeleteResponse response = ExecutorUtil.callWithRetry(() -> {
            return restHighLevelClient.delete(request, ElasticSearchClientFactory.getRequestOptions());
        }, RETRY_TIMES, (i, e)-> {
            if (i == RETRY_TIMES - 1) {
                logger.error("delete error, request={}, key={}, exception={}", request, key, e);
            }
            return true;
        });

        Result<String> r = getResult(request, response);
        return r;
    }


    /**
     * 批量写操作： bulk 请求不是原子的，不能用来实现事务控制，每个请求是单独处理的，钦此一个请求的失败不会影响其他的请求
     * 参见: https://www.elastic.co/guide/cn/elasticsearch/guide/current/bulk.html
     *
     * @param batchList 请求
     * @param key  ES配置属性
     * @param refreshPolicy 0=NONE 1=IMMEDIATE 2=WAIT_UNTIL
     * @return
     */
    public static BulkResponse batchWriteDocs(List<DocWriteRequest<?>> batchList, String key, int refreshPolicy){
        if (CollectionUtils.isEmpty(batchList)) {
            throw new IllegalArgumentException("batchList is null or empty");
        }

        RestHighLevelClient restHighLevelClient = ElasticSearchClientFactory.getClient(key);
        BulkRequest request = new BulkRequest();
        for (DocWriteRequest<?> item : batchList) {
            request.add(item);
        }

        // 索引刷新策略 更新后是否立即刷新索引
        if (refreshPolicy == 0){
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        } else if (refreshPolicy == 1) {
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        } else if (refreshPolicy == 2) {
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }

        BulkResponse response = ExecutorUtil.callWithRetry(() -> {
            return restHighLevelClient.bulk(request, ElasticSearchClientFactory.getRequestOptions());
        }, RETRY_TIMES, (i, e) -> {
            if (i == RETRY_TIMES - 1) {
                logger.error("batchWriteDocs error, request={}, key={}, exception={}", request, key, e);
            }
            return true;
        });

        if (response == null) {
            throw new RuntimeException("batchWriteDocs error");
        }

        return response;
    }



}
