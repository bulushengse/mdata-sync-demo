package com.zhoubc.mdata.sync.tconfig;

import lombok.Data;

import java.util.List;

/**
 * ES索引配置模型   classpath*:/config/*.json
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 22:06
 */
@Data
public class OutputConfig extends BaseDefination {
    //ES索引表名称
    private String destination;

    //ES索引表关联的数据库表
    private List<ModelDefination> modelList;

    //ES索引对应的DB多表关联语句   ["","",""]
    private List<String> relationList;

    //？
    private String returnStatement;

    //？
    private String tableDataBacktrackingHandler;

    //？
    private int options = 1;

    private String preconditionExpression;

    // "item.cz_sc_item,trade.trade_order,store.xxx"
    private String ignoredTablesWhenExecution;

    private String customizedScript;

    private String title;

    private Boolean disabled = null;

}
