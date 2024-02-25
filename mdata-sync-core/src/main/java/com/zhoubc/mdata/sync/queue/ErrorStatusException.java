package com.zhoubc.mdata.sync.queue;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/16 22:43
 */
public class ErrorStatusException extends RuntimeException {
    private static final long serialVersionUID = -7085405868744699288L;

    public ErrorStatusException(String message){
        super(message);
    }

}
