package com.zhoubc.mdata.sync.tbuilder.model;

import lombok.Data;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/10/28 21:59
 */
@Data
public class Condition {

    private String left;

    private int leftTypeCode;

    private String leftTags;

    private String operator;  //操作符 =

    private String right;

    private int rightTypeCode;

    private String rightTags;


    public static Condition of(String left, String operator, String right){
        return of(left, 0, operator, right, 0);
    }

    public static Condition of(String left, int leftTypeCode, String operator, String right, int rightTypeCode){
        Condition r = new Condition();
        r.left = left;
        r.leftTypeCode = leftTypeCode;
        r.operator = operator;
        r.right = right;
        r.rightTypeCode = rightTypeCode;
        return r;
    }

}
