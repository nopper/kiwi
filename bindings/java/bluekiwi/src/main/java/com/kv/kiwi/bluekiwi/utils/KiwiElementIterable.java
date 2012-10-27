package com.kv.kiwi.bluekiwi.utils;

import com.kv.kiwi.bluekiwi.KiwiGraph;

public class KiwiElementIterable {
    protected KiwiElementType.TYPE type;
    protected KiwiGraph graph;
    protected String key;
    protected Object value;

    public KiwiElementIterable(KiwiElementType.TYPE type, KiwiGraph graph,
            String key, Object value) {
        this.type = type;
        this.graph = graph;
        this.key = key;
        this.value = value;
    }
}
