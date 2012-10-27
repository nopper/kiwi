package com.kv.kiwi.bluekiwi.utils;

import java.util.Iterator;

import com.kv.kiwi.bluekiwi.KiwiGraph;
import com.tinkerpop.blueprints.Vertex;

public class KiwiVertexIterable extends KiwiElementIterable implements
        Iterable<Vertex> {
    public KiwiVertexIterable(final KiwiGraph graph) {
        this(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX, graph, null, null);
    }

    public KiwiVertexIterable(final KiwiGraph graph, String key, Object value) {
        this(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX, graph, key, value);
    }

    public KiwiVertexIterable(final KiwiElementType.TYPE type,
            final KiwiGraph graph, String key, Object value) {
        super(type, graph, key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Vertex> iterator() {
        return new KiwiVertexIterator(graph, key, value);
    }

}
