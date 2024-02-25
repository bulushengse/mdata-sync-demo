package com.zhoubc.mdata.sync.queue;

import lombok.Data;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/9 18:48
 */
@Data
public class QueueData<T> {
    private int type;
    private T data;

    public QueueData() {
    }

    public static <T> QueueData<T> of(T data) {
        return of(0, data);
    }

    public static <T> QueueData<T> of(int type, T data) {
        QueueData x = new QueueData();
        x.type = type;
        x.data = data;
        return x;
    }
}
