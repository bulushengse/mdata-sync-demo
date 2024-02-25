package com.zhoubc.mdata.sync.utils;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/30 18:40
 */
public class StringPair extends Pair<String, String> {

    public StringPair(String left, String right) {
        super(left, right);
    }

    public static StringPair of(String left, String right) {
        return new StringPair(left, right);
    }


}
