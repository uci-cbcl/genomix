package edu.uci.ics.genomix.data.test;

import java.util.Map.Entry;


public class EdgeWritable<K, V> implements Entry<K, V> {
    
    private final K key;
    private V value;

    public EdgeWritable(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }
}
