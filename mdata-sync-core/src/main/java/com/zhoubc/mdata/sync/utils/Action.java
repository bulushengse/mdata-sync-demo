package com.zhoubc.mdata.sync.utils;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/9 18:35
 */
@FunctionalInterface
public interface Action<T> {
    void execute(T var1);
}
