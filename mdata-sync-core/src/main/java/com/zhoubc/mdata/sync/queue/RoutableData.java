package com.zhoubc.mdata.sync.queue;


import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 队列处理模型
 * @author zhoubc
 * @description: TODO
 * @date 2023/7/9 18:38
 */
@Data
public class RoutableData {
    private String caller;    // 线程名

    private String endpoint;  // endpoint
    private Object userData;  // 线程数据 instanceof CountDownLatch同步工具类

    private Map<String, Object> args = new HashMap();

    public RoutableData() {
    }

    public static RoutableData of(String caller, String rd, Object ud, Map<String, Object> args) {
        RoutableData x = new RoutableData();
        x.caller = caller;
        x.endpoint = rd;
        x.userData = ud;
        x.args = args;
        return x;
    }

    public static RoutableData of(String caller, String rd, Object ud) {
        RoutableData x = new RoutableData();
        x.caller = caller;
        x.endpoint = rd;
        x.userData = ud;
        return x;
    }


}
