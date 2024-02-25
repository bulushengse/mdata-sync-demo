package com.zhoubc.mdata.sync.tbuilder.model;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/10/29 22:55
 */
public class Equation extends Condition{
    private static final String EQ = "=";

    public static Equation of(String left, String right) {
        Equation r = new Equation();
        r.setLeft(left);
        r.setRight(right);
        return r;
    }

    public String getOperator() {
        return "=";
    }

    public void setOperator(String operator){
        throw new RuntimeException("操作符固定=，不允许另外设置");
    }


}
