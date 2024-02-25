package com.zhoubc.mdata.sync.tconfig;

import lombok.Data;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ES索引配置 数据库表模型
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 21:59
 */
@Data
public class ModelDefination extends BaseDefination {

    //？
    private String code;

    //数据库名
    private String dataSource;

    //表名
    private String tableName;

    //表字段
    private List<FieldDefination> fieldList;


    //获取主键字段名
    public String getPrimaryKey(){
        if (CollectionUtils.isEmpty(fieldList)) {
            return null;
        }

        List<FieldDefination> primaryFields = fieldList.stream().filter(x->x.getPrimaryKey()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(primaryFields)) {
            return null;
        }

        return primaryFields.get(0).getColumnName();
    }

    public String getDataSourceTableName(){
        return dataSource+"."+tableName;
    }




}
