package com.zhoubc.mdata.sync.utils;

import lombok.Data;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/4/2 18:27
 */
@Data
public class Result<T> {
    public static int OK = 200;
    public static int ERR = 500;
    private int code = -1;
    private T value;
    private String message;

    public Result(){}

    public Result(int c) {
        this.code = c;
    }

    public Result(int c, T v) {
        this.code = c;
        this.value = v;
    }

    public Result(int c, T v, String m){
        this.code = c;
        this.value = v;
        this.message = m;
    }

    public static <T> boolean isOK(Result<T> r) { return r != null && r.code == OK; }

    public static <T> Result<T> ok(T v) { return new Result(OK, v); }

    public static <T> Result<T> ok(T v, String message) { return of(OK, v, message); }

    public static <T> Result<T> err(int c) { return new Result(c, null, null); }

    public static <T> Result<T> err(int c, String message) { return new Result(c, null, message); }

    public static <T> Result<T> of(int c, T v) { return new Result(c, v); }

    public static <T> Result<T> of(int c, T v, String message) { return new Result(c, v, message); }

}
