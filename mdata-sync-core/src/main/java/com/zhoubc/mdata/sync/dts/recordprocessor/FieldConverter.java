package com.zhoubc.mdata.sync.dts.recordprocessor;

import com.alibaba.dts.formats.avro.Field;
import org.apache.commons.lang3.StringUtils;


public interface FieldConverter {
    FieldValue convert(Field field, Object o);
    public static FieldConverter getConverter(String sourceName, String sourceVersion) {
        if (StringUtils.endsWithIgnoreCase("mysql", sourceName)) {
            return new MysqlFieldConverter();
        } else if (StringUtils.endsWithIgnoreCase("oracle", sourceName)) {
            //return new OracleFieldConverter();
        } else if (StringUtils.endsWithIgnoreCase("postgresql", sourceName)
                || StringUtils.endsWithIgnoreCase("pg", sourceName)) {
            //return new PostgresqlFieldConverter();
        }  else {
            throw new RuntimeException("FieldConverter: only mysql supported for now");
        }
        return null;
    }

}
