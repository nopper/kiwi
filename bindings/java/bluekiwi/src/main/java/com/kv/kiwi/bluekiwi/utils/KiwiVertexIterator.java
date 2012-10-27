package com.kv.kiwi.bluekiwi.utils;

import com.kv.kiwi.bluekiwi.KiwiGraph;

public class KiwiVertexIterator extends KiwiElementIterator {
    public KiwiVertexIterator(final KiwiGraph graph, String key, Object value) {
        super(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX, graph, key, value);
    }
}
