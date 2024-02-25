package com.zhoubc.mdata.sync.tconfig;

import lombok.Data;

import java.util.Map;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 22:04
 */
@Data
public class BaseDefination {
    private Long id;

    private String version;

    private Integer status;

    private String description;

    private Map<String, Object> properties;

}
