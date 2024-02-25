package com.zhoubc.mdata.sync.tbuilder;

import com.zhoubc.mdata.sync.tconfig.ESDataSourceDefination;
import com.zhoubc.mdata.sync.tconfig.ModelDefination;
import com.zhoubc.mdata.sync.tconfig.OutputConfig;
import com.zhoubc.mdata.sync.utils.MapUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/9 20:26
 */
public class Environment {
    private static final Logger logger = LoggerFactory.getLogger(Environment.class);

    //ES表对应ParesdSchema   schemaKey（ES索引表名称）, ParesdSchema
    private static Map<String, ParsedSchema> paresdSchemaMap;

    //ES表对应应数据库表    schemaKey（ES索引表名称），dataSourceDefination
    private static Map<String, ESDataSourceDefination> tableDefinationMap;

    //数据库表对应关联的ES表   dbTable，schemaKeys
    private static Map<String, Set<String>> dbTableToSchemaKeyCache;

    //数据库表对应的主键字段名  dbTable，primaryKey
    private static Map<String, String> tablePrimaryKeyMap;


    static {
        paresdSchemaMap = MapUtil.concurrentHashMap();
        tableDefinationMap = MapUtil.concurrentHashMap();
        dbTableToSchemaKeyCache = MapUtil.concurrentHashMap();
        tablePrimaryKeyMap = MapUtil.concurrentHashMap();
    }

    /**
     * 注册数据源
     * @param cf
     */
    public static void registerSchema(OutputConfig cf){
        ESDataSourceDefination dataSourceDefination = new ESDataSourceDefination();
        dataSourceDefination.setSchema(cf.getDestination());//ES索引表名称
        dataSourceDefination.setIndexName(cf.getDestination());
        dataSourceDefination.setEndpoint("127.0.0.1:9200");//127.0.0.1:9092
        dataSourceDefination.setOutputConfig(cf);
        tableDefinationMap.put(cf.getDestination(),dataSourceDefination);

//        ParsedSchema paresdSchema = new ParsedSchema();
//        paresdSchemaMap.put(cf.getDestination(),paresdSchema);


        for (ModelDefination defination : cf.getModelList()) {
            String dbTable = defination.getDataSourceTableName();
            Set<String> schemaKeys = dbTableToSchemaKeyCache.get(dbTable);
            if(schemaKeys == null){
                schemaKeys = new HashSet<>();
            }
            schemaKeys.add(cf.getDestination());
            dbTableToSchemaKeyCache.put(dbTable, schemaKeys);

            String primaryKeyColumnName = defination.getPrimaryKey();
            if(StringUtils.isNotEmpty(primaryKeyColumnName)){
                tablePrimaryKeyMap.put(dbTable, primaryKeyColumnName);
            }else{
                logger.warn("registerSchema注意：未找到表{}的主键", dbTable);
            }
        }
    }

    /**
     * 获取ES数据源定义
     * @param schemaKey
     * @return
     */
    public static ESDataSourceDefination getDataSourceDefination(String schemaKey) {
        if(StringUtils.isBlank(schemaKey)){
            return null;
        }
        return tableDefinationMap.get(schemaKey);
    }

    /**
     * 获取数据库表关联的ES表
     * @param tableName
     * @return
     */
    public static Set<String> getIndexNameSet(String tableName){
        if(StringUtils.isBlank(tableName)){
            return null;
        }
        return dbTableToSchemaKeyCache.get(tableName);
    }

    /**
     * 获取数据库表主键
     * @param tableName
     * @return
     */
    public static String getTablePrimaryKey(String tableName){
        if(StringUtils.isBlank(tableName)){
            return null;
        }
        return tablePrimaryKeyMap.get(tableName);
    }

    /**
     * 获取ParsedSchema
     * @param schemaKey
     * @return
     */
    public static ParsedSchema getParsedSchema(String schemaKey){
        if(StringUtils.isBlank(schemaKey)){
            return null;
        }
        return paresdSchemaMap.get(schemaKey);
    }



    public static void processOneSchema(){



        //ParsedSchema parsedSchema = getParsedSchema();


        //回溯？


    }



}
