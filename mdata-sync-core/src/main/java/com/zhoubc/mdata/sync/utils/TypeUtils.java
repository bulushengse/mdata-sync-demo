package com.zhoubc.mdata.sync.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/10/30 1:13
 */
public class TypeUtils {
    public static final String Y = "y";
    public static final String N = "n";
    public static BasicType STRING_TYPE = BasicType.of("string", 12, "varchar,char,text,keyword,longtext", new int[]{1, 65535});
    public static BasicType INT_TYPE = BasicType.of("int", 4, "integer,snallint,tinyint", new int[]{1, 10});
    public static BasicType DECIMAL_TYPE = BasicType.of("decimal", 3, "number,float,double,bigdecimal", new int[]{1, 20});
    public static BasicType LONG_TYPE = BasicType.of("long", -5, "bigint", new int[]{1, 20});
    public static BasicType DATE_TIME_TYPE = BasicType.of("date", 91, "datetime,timestamp", new int[]{1, 19});
    public static BasicType BOOLEAN_TYPE = BasicType.of("boolean", 16, "bool", new int[]{1, 5});
    public static BasicType OBJECT_TYPE = BasicType.of("object", 0, "json,blob", new int[]{1, 65535});

    public static Map<String, BasicType> typeMap = MapUtil.concurrentHashMap();
    public static Map<Integer, BasicType> typeCodeMap = MapUtil.concurrentHashMap();


    static {
        BasicType[] tarr = new BasicType[]{STRING_TYPE,INT_TYPE,DECIMAL_TYPE,LONG_TYPE,DATE_TIME_TYPE,BOOLEAN_TYPE,OBJECT_TYPE};

        for (int i = 0; i < tarr.length; i++) {
            BasicType bt = tarr[i];
            typeMap.put(bt.getName().toLowerCase(), bt);
            typeCodeMap.put(bt.getCode(), bt);
            if (StringUtils.isNotEmpty(bt.getAliases())) {
                String[] arr = StringUtils.split(bt.getAliases(), ",");

                for (int j = 0; j < arr.length; j++) {
                    String aname = arr[j];
                    typeMap.put(aname.toLowerCase(), bt);
                }
            }
        }
    }

    public static BasicType getBasicType(String x){
        if (StringUtils.isBlank(x)) {
            return null;
        } else {
            BasicType bt = typeMap.get(x);
            return bt != null? bt : typeMap.get(x.toLowerCase());
        }
    }

    public static int getTypeCode(String x){
        if (StringUtils.isBlank(x)) {
            return 0;
        } else {
            BasicType bt = getBasicType(x);
            return bt != null ? bt.getCode() : 0;
        }
    }

}
