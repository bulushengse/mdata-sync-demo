package com.zhoubc.mdata.sync.dts.recordprocessor;

import com.alibaba.dts.formats.avro.Operation;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/3/18 19:38
 */
@Data
public class ChangeLog {
    private String dbName;

    private String tableName;

    private Operation operation;

    private Map<String,Object> beforeFieldMap;

    private Map<String,Object> afterFieldMap;

    private long bornTimestamp;

    private long recordId;

    private String objectName;//"dbName.tableName"

    private String routing;

    public Object pk;
    public Future<?> future;











}
