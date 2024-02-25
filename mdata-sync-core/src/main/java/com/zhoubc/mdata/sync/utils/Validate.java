package com.zhoubc.mdata.sync.utils;

import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 17:49
 */
public class Validate {

    public static String assertNotEmpty(String obj, String errorMsg){
        if(obj != null && obj.length() != 0){
            return obj;
        }else{
            throw new IllegalArgumentException(errorMsg);
        }
    }


    public static <T> T assertNotNull(T obj, String errorMsg){
        if(obj != null){
            return obj;
        }else{
            throw new IllegalArgumentException(errorMsg);
        }
    }

    public static Boolean assertNotEmptyList(List list, String errorMsg){
        if(CollectionUtils.isEmpty((list))){
            return true;
        }else{
            throw new IllegalArgumentException(errorMsg);
        }
    }


    public static void assertIsTrue(Boolean expression, String errorMsg){
        if (!expression) {
            throw new IllegalArgumentException(errorMsg);
        }
    }



}
