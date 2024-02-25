package com.zhoubc.mdata.sync.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/2/12 17:59
 */
public class MapUtil {


    public static <K, V> int getInt(Map<?, ?> map, K key, int def){
        return key != null && map != null ? Integer.valueOf(map.get(key).toString()) : def;
    }

    public static <K, V> String getString(Map<?, ?> map, K key){
        return key != null && map != null ? map.get(key).toString() : null;
    }


    public static <K,V> V getValue(Map<K, V> m, K k, Func<K, V> valueGetter){
        if (m == null) {
            throw new RuntimeException("");
        } else if (k == null){
            throw new RuntimeException("");
        } else {
            V v = m.get(k);
            if (v == null && valueGetter != null) {
                synchronized (m) {
                    v = m.get(k);
                    if (v == null) {
                        v = valueGetter.execute(k);
                        m.put(k, v);
                    }
                }
            }
            return v;
        }
    }

    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap() {
        return concurrentHashMap(32);
    }

    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap(int initialCapacity) {
        return new ConcurrentHashMap<>(getInitialCapacity(initialCapacity));
    }

    public static <K, V> HashMap<K, V> map() {
        return new HashMap(getInitialCapacity(32));
    }

    public static <T> Set<T> set() {
        return new HashSet(getInitialCapacity(32));
    }

    public static int getInitialCapacity(int size) {
        return (int)((float)size / 0.75F) + 1;
    }


    public static <K, V> List<V> addToList(Map<K, List<V>> map, K key, V v) {
        List<V> r = getValue(map, key, (k) -> {
            return new ArrayList<>();
        });
        r.add(v);
        return r;
    }

}
