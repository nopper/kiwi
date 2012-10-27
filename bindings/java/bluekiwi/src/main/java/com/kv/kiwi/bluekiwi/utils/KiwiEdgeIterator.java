package com.kv.kiwi.bluekiwi.utils;

import com.kv.kiwi.bluekiwi.KiwiGraph;

public class KiwiEdgeIterator extends KiwiElementIterator {
    public KiwiEdgeIterator(final KiwiGraph graph, String key, Object value) {
        super(KiwiElementType.TYPE.KIWI_ELEMENT_EDGE, graph, key, value);
    }
}
