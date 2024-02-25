package com.zhoubc.mdata.sync.tbuilder.model;

import lombok.Data;

import java.util.List;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/10/15 19:42
 */
@Data
public class NodeInfo {

    private int flag;
    private String id;
    private List<String> path;
    private String extra;
    private String[] stringPool = new String[]{null,null,null,null,null};


    public static NodeInfo of(int f, String id, List<String> path, String extra){
        NodeInfo x = new NodeInfo();
        x.flag = f;
        x.id = id;
        x.path = path;
        x.extra = extra;
        return x;
    }

}
