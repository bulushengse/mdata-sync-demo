package com.zhoubc.mdata.sync.utils;

import java.util.Objects;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 18:36
 */
public class Pair<L, R> {
    private L left;
    private R right;
    private int hashCode = 0;

    public Pair() {
    }

    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public static <L, R> Pair<L, R> of(L left, R right){
        return new Pair(left, right);
    }

    public L getLeft() {
        return this.left;
    }

    public void setLeft(L left) {
        this.hashCode = 0;
        this.left = left;
    }

    public R getRight() {
        return this.right;
    }

    public void setRight(R right) {
        this.hashCode = 0;
        this.right = right;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof Pair)) {
            return false;
        } else {
            Pair<?, ?> other = (Pair)obj;
            return Objects.equals(this.left, other.getLeft()) && Objects.equals(this.right, other.getRight());
        }
    }


}
