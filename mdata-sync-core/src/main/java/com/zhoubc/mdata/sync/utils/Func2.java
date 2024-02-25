package com.zhoubc.mdata.sync.utils;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 18:30
 */
@FunctionalInterface
public interface Func2<T1, T2, TResult> {
    TResult execute(T1 var1, T2 var2);
}
