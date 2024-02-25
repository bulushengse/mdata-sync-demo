package com.zhoubc.mdata.sync.tconfig;

import lombok.Data;

/**
 * ES索引配置 数据库表字段模型
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 22:00
 */
@Data
public class FieldDefination extends BaseDefination {

    //表字段是否是主键
    private Boolean primaryKey;

    //表字段名
    private String columnName;

    //是否为ES索引字段
    private Boolean outerIndexed;

    //ES索引字段数据类型
    private String outerIndexDataType;

    //ES索引名
    private String outerIndexName;

    //？
    private String outerIndexSettings;

    //索引值为特殊处理调用方法，如数据表多个字段组合构成、features字段里的字段值等等
    private String translation;

    //分片字段
    //private Boolean shardingKey;

}
