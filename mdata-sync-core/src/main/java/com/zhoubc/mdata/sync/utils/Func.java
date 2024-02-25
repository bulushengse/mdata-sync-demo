package com.zhoubc.mdata.sync.utils;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 18:30
 */
@FunctionalInterface
public interface Func<T, TResult> {
    TResult execute(T var1);
}
