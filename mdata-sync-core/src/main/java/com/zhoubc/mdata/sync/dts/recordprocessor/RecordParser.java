package com.zhoubc.mdata.sync.dts.recordprocessor;

import com.alibaba.dts.formats.avro.Field;
import com.alibaba.dts.formats.avro.Operation;
import com.alibaba.dts.formats.avro.Record;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/3/18 20:24
 */
public class RecordParser {
    private final Logger logger = LoggerFactory.getLogger(RecordParser.class);
    private static final List<Operation> DML_OPERATIONS = Stream.of(Operation.INSERT,Operation.UPDATE,Operation.DELETE).collect(Collectors.toList());
    private static final FieldConverter FIELD_CONVERTER = FieldConverter.getConverter("mysql",null);


    public static ChangeLog parse(Record record, long bornTimestamp, long offset){
        if (record == null) {
            return null;
        }

        if(!isDML(record)){//INSERT,UPDATE,DELETE?
            return null;
        }

        boolean isDelete = record.getOperation() == Operation.DELETE;

        Tuple<String, String> dbNameAndTableName = getDbNameAndTableName(record);
        Map<String, Object> beforeFiledMap = isDelete ? new HashMap<>() : null;
        Map<String, Object> afterFiledMap = isDelete ? null : new HashMap<>();

        fillFieldMap(record, beforeFiledMap, afterFiledMap);

        ChangeLog changeLog = new ChangeLog();
        changeLog.setOperation(record.getOperation());
        changeLog.setDbName(dbNameAndTableName.v1());
        changeLog.setTableName(dbNameAndTableName.v2());
        changeLog.setBeforeFieldMap(beforeFiledMap);
        changeLog.setAfterFieldMap(afterFiledMap);
        changeLog.setBornTimestamp(bornTimestamp);
        changeLog.setRecordId(offset);
        return  changeLog;
    }

    private static void fillFieldMap(Record record, Map<String, Object> beforeFiledMap, Map<String, Object> afterFiledMap){
        List<Field> fields = (List<Field>) record.getFields();
        FieldEntryHolder before = new FieldEntryHolder((List<Object>) record.getBeforeImages());
        FieldEntryHolder after = new FieldEntryHolder((List<Object>) record.getAfterImages());

        if (null != fields) {
            Iterator<Field> fieldIterator = fields.iterator();
            while (fieldIterator.hasNext() && before.hasNext() && after.hasNext()) {
                Field field = fieldIterator.next();
                Object toPrintBefore = before.take();
                Object toPrintAfter = after.take();

                FieldValue beforeValue = FIELD_CONVERTER.convert(field,toPrintBefore);
                String beforeValueString = beforeValue.getValue() == null ? null : beforeValue.toString();
                beforeFiledMap.put(field.getName(), beforeValueString);

                FieldValue afterValue = FIELD_CONVERTER.convert(field,toPrintAfter);
                String afterValueString = afterValue.getValue() == null ? null : afterValue.toString();
                afterFiledMap.put(field.getName(), afterValueString);
            }

        }
    }

    private static boolean isDML(Record record) {
        return DML_OPERATIONS.contains(record.getOperation());
    }


    private static Tuple<String, String> getDbNameAndTableName(Record record){
        String dbName = null;
        String tableName = null;
        String[] dbPair = split(record.getObjectName());

        if (null != dbPair) {
            if (dbPair.length == 2) {
                dbName = dbPair[0];
                tableName = dbPair[1];
            } else if (dbPair.length == 3) {
                dbName = dbPair[0];
                tableName = dbPair[2];
            } else if (dbPair.length == 1) {
                dbName = dbPair[0];
                tableName = "";
            } else {
                throw new RuntimeException("invalid db and table name pair for record [" + record + "]");
            }
        }
        return new Tuple<>(dbName, tableName);
    }

    private static String[] split(String s){
        if (null == s || s.isEmpty()) {
            return null;
        }

        String[] names = s.split("\\.");
        int length = names.length;

        for (int i = 0; i < length; ++i){
            names[i] = unescapeName(names[i]);
        }

        return names;
    }

    private static String unescapeName(String name) {
        if (null == name || (!name.contains("\\u002E"))) {
            return name;
        }

        StringBuilder builder = new StringBuilder();
        int length = name.length();

        for (int i = 0; i < length; i++) {
            char c = name.charAt(i);
            if ('\\' == c && (i < length - 6 && 'u' == name.charAt(i + 1) && '0' == name.charAt(i + 2)
                    && '0' == name.charAt(i + 3) && '2' == name.charAt(i + 4) && 'E' == name.charAt(i + 5))) {
                builder.append(".");
                i += 5;
            } else {
                builder.append(c);
            }
        }

        return builder.toString();
    }



}
