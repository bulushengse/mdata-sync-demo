package com.zhoubc.mdata.sync.dts.recordprocessor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/3/18 20:43
 */
public class FieldEntryHolder {
    private final Iterator<Object> iterator;
    private final List<Object> filteredFields;


    public FieldEntryHolder(List<Object> originFields) {
        if (null == originFields) {
            this.filteredFields = null;
            this.iterator = null;
        } else{
            this.filteredFields = new LinkedList<>();
            this.iterator = originFields.iterator();
        }
    }

    public boolean hasNext() {
        if (iterator == null) {
            return true;
        }
        return iterator.hasNext();
    }

    public Object take() {
        if (null != iterator) {
            Object current = iterator.next();
            filteredFields.add(current);
            return current;
        } else {
            return null;
        }
    }

}
