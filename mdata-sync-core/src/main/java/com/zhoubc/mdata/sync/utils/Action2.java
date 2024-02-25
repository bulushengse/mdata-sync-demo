package com.zhoubc.mdata.sync.utils;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/9 18:35
 */
@FunctionalInterface
public interface Action2<T1, T2> {
    void execute(T1 var1, T2 var2);
}
