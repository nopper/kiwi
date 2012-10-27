package com.kv.kiwi.bluekiwi.utils;

import java.util.Iterator;

import com.kv.kiwi.bluekiwi.KiwiGraph;
import com.tinkerpop.blueprints.Edge;

public class KiwiEdgeIterable extends KiwiElementIterable implements
        Iterable<Edge> {
    public KiwiEdgeIterable(final KiwiGraph graph) {
        this(KiwiElementType.TYPE.KIWI_ELEMENT_EDGE, graph, null, null);
    }

    public KiwiEdgeIterable(final KiwiGraph graph, String key, Object value) {
        this(KiwiElementType.TYPE.KIWI_ELEMENT_EDGE, graph, key, value);
    }

    public KiwiEdgeIterable(final KiwiElementType.TYPE type,
            final KiwiGraph graph, String key, Object value) {
        super(type, graph, key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Edge> iterator() {
        return new KiwiEdgeIterator(graph, key, value);
    }

}
