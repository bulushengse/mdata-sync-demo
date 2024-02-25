package com.zhoubc.mdata.sync.utils;

import lombok.Data;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/10/30 1:13
 */
@Data
public class BasicType {
    private String name;
    private int code;
    private String aliases;
    private int[] lengthRange;
    private int[] jdbcNullValue;


    public static BasicType of(String name,int code,String aliases,int[] lengthRange) {
        BasicType t = new BasicType();
        t.name = name;
        t.code = code;
        t.aliases = aliases;
        t.lengthRange = lengthRange;
        t.jdbcNullValue = new int[]{57526, code};
        return t;
    }

}
