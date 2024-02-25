package com.zhoubc.mdata.sync.tconfig;

import lombok.Data;

/**
 * 数据源定义
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 21:48
 */
@Data
public class ESDataSourceDefination extends BaseDefination {
//    private String dataSourceType; //“mysql”、”elasticsearch”、

      private String endpoint; //host：port
//    private String userName;
//    private String password;
//    private String connectProperties;

    private String schema; //config下的ES配置文件名
    private String indexName;//ES索引表名称outputConfig.destination，理论上跟schema配置文件名一样的


    private OutputConfig outputConfig;


}
