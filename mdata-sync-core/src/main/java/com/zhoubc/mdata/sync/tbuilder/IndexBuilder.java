package com.zhoubc.mdata.sync.tbuilder;


import com.alibaba.dts.formats.avro.Operation;
import com.google.common.collect.Lists;
import com.zhoubc.mdata.sync.dts.recordprocessor.AvroDeserializer;
import com.zhoubc.mdata.sync.dts.recordprocessor.ChangeLog;
import com.zhoubc.mdata.sync.es.EsDocWriter;
import com.zhoubc.mdata.sync.utils.StringPair;
import com.zhoubc.mdata.sync.tconfig.ESDataSourceDefination;
import com.zhoubc.mdata.sync.queue.RoutableData;
import com.zhoubc.mdata.sync.utils.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/5 19:49
 */
public class IndexBuilder {
    private static final Logger logger = LoggerFactory.getLogger(IndexBuilder.class);
    private static final AvroDeserializer AVRO_DESERIALIZER = new AvroDeserializer();

    private static ExecutorService esWriteThreadPool = ThreadPoolUtils.buildESWriteThreadPool();

    private static final String ID_KW = "_id";
    private static final String DOC_KW = "_doc";
    private static final String DESTINATION_KW = "__destination";
    private static final String OP_KW = "__op";
    private static final String REFRESH_IMMEDIATELY_KW = "refreshImmediately";
    private static final String DOC_TYPE_KW = "doc_type";
    private static final String SAME_AS_INDEX_KW = "same_as_index";
    private static final String ELASTIC_SEARCH_KW = "elasticsearch";

    private static final int INDEX_OP = 0;
    private static final int DELETE_OP = 3;

    private static boolean INDEX_REFRESH_IMMEDIATELY = false;

    public static void batchHandler(List<RoutableData> list){
        //去重容器， key=(destination + _id)
        Set<StringPair> set = MapUtil.set();

        Map<String, List<DocWriteRequest>> mlist = MapUtil.map();
        int size = list.size();

        for (int i = size - 1; i >= 0; i--) {//倒序遍历，有重复时后面的覆盖前面的，其实没有关系，并不是说后面记录比前面新，这取决于并行构建的速度
            RoutableData item = list.get(i);

            Map<String, Object> mdoc = (Map<String, Object>) item.getUserData();

            ESDataSourceDefination ds = (ESDataSourceDefination) mdoc.remove(DESTINATION_KW);//返回此映射以前与键关联的值，如果映射不包含键的映射，则返回null。
            int operationType = (int) mdoc.remove(OP_KW);
            String id = (String) mdoc.remove(ID_KW);

//            // 判重 为了记录不相互冲突
//            StringPair key = StringPair.of(ds.getUniqueKey(), id);
//            if (set.contains(key)) {
//                continue;
//            }
//            // 按endpoint来归类
//            set.add(key);

            String endpoint = item.getEndpoint();     //es目的地，endpoint纬度
            DocWriteRequest<?> doc = toESDoc(ds,id,mdoc,operationType);
            if (doc != null) {
                MapUtil.addToList(mlist, endpoint, doc);
            }
        }

        // 拆分小批量写入es
        List<Future<?>> futureList = new ArrayList<>();
        for (Map.Entry<String, List<DocWriteRequest>> m : mlist.entrySet()) {
            String endpoint = m.getKey();
            List<DocWriteRequest> reqList = m.getValue();
            List<List<DocWriteRequest>> batchReqList = Lists.partition(reqList,256);

            for (List writeRequests : batchReqList) {
                Future<?> f = CompletableFuture.supplyAsync(() -> {
                    return batchWriteDocs(writeRequests, endpoint);
                }, esWriteThreadPool);

                futureList.add(f);
            }
        }
        // 等待线程池执行完毕
        for (Future<?> future : futureList) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.info("写入es返回异常了", e);
            }
        }

        //清理
        set = null;
        mlist = null;

        //log..
    }

    private static DocWriteRequest<?> toESDoc(ESDataSourceDefination esDefine, String id, Map<String, Object> docMap, int operationType) {
        if (esDefine == null) {
            return null;
        }
        if (docMap == null) {
            return null;
        }
        if (id == null) {
            return null;
        }

        String index = esDefine.getSchema();
        String type = DOC_KW;
        boolean refreshImmediately = INDEX_REFRESH_IMMEDIATELY;

        // 特殊的配置
        Map<String, Object> indexSettings = esDefine.getProperties();
        if (!CollectionUtils.isEmpty(indexSettings)){
            Object temp = indexSettings.get(REFRESH_IMMEDIATELY_KW);
            if (temp != null) {
                refreshImmediately = (boolean) temp;
            }

            String docType = MapUtil.getString(indexSettings, DOC_TYPE_KW);
            if (SAME_AS_INDEX_KW.equals(docType)) {
                type = index;
            }
        }

        // 创建对象
        DocWriteRequest<?> r = null;

        if (operationType == INDEX_OP) {
            if (docMap.size() == 0) {
                return null;
            }

            IndexRequest indexRequest = new IndexRequest();
            if (refreshImmediately) {
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            indexRequest.index(index);
            indexRequest.type(type);
            indexRequest.id(id);
            indexRequest.opType(DocWriteRequest.OpType.INDEX);
            indexRequest.source(docMap, XContentType.JSON);
            r = indexRequest;

        } else if (operationType == DELETE_OP) {
            DeleteRequest deleteRequest = new DeleteRequest();
            if (refreshImmediately) {
                deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            deleteRequest.index(index);
            deleteRequest.type(type);
            deleteRequest.id(id);
            r = deleteRequest;
        } else {

            return null;
        }

        return r;
    }

    private static boolean batchWriteDocs(List<DocWriteRequest<?>> requestList, String endpoint) {
        if (requestList == null) {
            return false;
        }

        int size = requestList.size();
        // 处理单条记录
        if (size == 1) {
            DocWriteRequest<?> request = requestList.get(0);
            boolean success = false;
            Result<String> r = null;

            if (request instanceof IndexRequest) {
                r = EsDocWriter.indexDoc((IndexRequest) request, endpoint);
                success = Result.isOK(r);
            } else if (request instanceof DeleteRequest) {
                r = EsDocWriter.deleteDoc((DeleteRequest) request, endpoint);
                success = Result.isOK(r);
            } else {
                return false;
            }

            if (!success) {
                String failureMessage = r != null ? r.getMessage() : null;
                //log.error
            }
            return success;
        }

        // 处理多条记录，bulk提交
        int refreshPoilcy = 0; //index refresh immediately
        BulkResponse responses = EsDocWriter.batchWriteDocs(requestList, endpoint, refreshPoilcy);
        for (BulkItemResponse response : responses.getItems()) {
//            String operationType = response.getOpType().ordinal() == 0 ? "I" : "D";
//            String type = "_doc".equals(response.getType()) ? "_" : response.getType();

            if (response.isFailed()) {
                //log.error
            } else {

            }
        }

        return true;
    }

    public static ChangeLog toChangeLog(ConsumerRecord<byte[],byte[]> consumerRecord){
//        Record record = AVRO_DESERIALIZER.deserialize(consumerRecord.value());
//
//        if(record == null){
//            return null;
//        }
//        //dbname.tablename  如cr_user_rlba_0000.event_process_action_record
//        String objectName = record.getObjectName();
//        if(objectName == null){
//            return null;
//        }

        //ChangeLog changeLog = RecordParser.parse(record,consumerRecord.timestamp(),consumerRecord.offset());

        //TODO mock changeLog test
        ChangeLog changeLog = new ChangeLog();
        changeLog.setDbName("ots");
        changeLog.setTableName("mdate_sync_test");
        changeLog.setOperation(Operation.UPDATE);
        Map<String,Object> afterFieldMap = new HashMap<>();
        afterFieldMap.put("id",Math.random());
        //afterFieldMap.put(ID_KW,"111111111");
        changeLog.setAfterFieldMap(afterFieldMap);
        changeLog.setBornTimestamp(1);
        changeLog.setRecordId(1);
        changeLog.setObjectName("ots.mdate_sync_test");
        return changeLog;
    }

    /**
     * BiConsumer<T,U>：代表了一个接受两个输入参数的操作，并且不返回任何结果
     * BiConsumer.accept()方法：用于接受参数并执行操作。
     */
    public static void buildSyncData(ChangeLog changeLog, BiConsumer<String, Map<String,Object>> queueHandler){
        if (changeLog == null) {
            return;
        }

        String tableName = changeLog.getObjectName();
        //db.table == null
        if (tableName == null) {
            logger.error("找不到表：{}.{}, 无法构建索引", changeLog.getDbName(), changeLog.getTableName());
            return;
        }

        Operation operation = changeLog.getOperation();
        int operationType = 0;
        Map<String, Object> updateFieldValue = null;
        if (operation.equals(Operation.INSERT) || operation.equals(Operation.UPDATE)) {
            operationType = INDEX_OP;
            updateFieldValue = changeLog.getAfterFieldMap();
        } else if (operation.equals(Operation.DELETE)) {
            operationType = DELETE_OP;
            updateFieldValue = changeLog.getBeforeFieldMap();
        } else {
            return;
        }

        int finalOperationType = operationType;
        //判断数据在不在配置里，要不要处理
        executeGetIndex(tableName, updateFieldValue, (indexName, updateDataMap) -> {
            //这里回调函数，要处理的逻辑
            ESDataSourceDefination defination = Environment.getDataSourceDefination(indexName);
            if (defination == null) {
                logger.warn("未找到目标数据源：destination={}", indexName);
                return;
            }

            updateDataMap.put(DESTINATION_KW, defination);
            updateDataMap.put(OP_KW, finalOperationType);

            //放入队列
            queueHandler.accept(defination.getEndpoint(), updateDataMap);
        });

    }

    /**
     * 根据tableName找到关联的所有ES,有则执行回调
     * @param tableName
     * @param updateFieldValue
     * @param callback
     */
    public static void executeGetIndex(String tableName, Map<String, Object> updateFieldValue, BiConsumer<String, Map<String, Object>> callback){
        // 根据数据库表找到关联的ES
        Set<String> indexNameSet = Environment.getIndexNameSet(tableName);

        if (indexNameSet == null) {
            // 找不到关联的ES，log打印
            // logger.error();
        } else {
            // 这里可以多线程处理

            // 暂时单线程处理
            // 数据库表关联的es，全部更新
            indexNameSet.stream().forEach(indexName -> {

                //

                //TODO    回溯未完成
                //要更新的表数据，其表不是主表，， 通过回溯 查询数据库的主表数据  生成完整的es数据(主要是id)  最后去同步es，
                //是主表不用回溯
                //详情在ParesdSchema.class

                //在index.json表中modelList  第一个表的配置一定要是主表

                //根基index.json的配置 有着丰富的功能 如features字段解析、多字段合并等等  需要Groovy脚本  生成es数据
                //ParesdSchema.class  304行


                //processOneSchema();

                ESDataSourceDefination esDataSourceDefination = Environment.getDataSourceDefination(indexName);

                if (esDataSourceDefination == null) {
                    // logger.error();
                } else {

                    callback.accept(esDataSourceDefination.getIndexName(), updateFieldValue);
                }





            });
        }
    }


}
