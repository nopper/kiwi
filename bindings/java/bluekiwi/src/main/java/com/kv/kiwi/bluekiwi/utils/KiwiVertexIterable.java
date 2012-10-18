package com.kv.kiwi.bluekiwi.utils;

import java.util.Iterator;

import com.kv.kiwi.bluekiwi.KiwiElement;
import com.kv.kiwi.bluekiwi.KiwiGraph;
import com.tinkerpop.blueprints.Vertex;

public class KiwiVertexIterable extends KiwiElementIterable implements Iterable<Vertex> {
	public KiwiVertexIterable(final KiwiGraph graph) {
		this(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX, graph, null);
	}

	public KiwiVertexIterable(final KiwiElementType.TYPE type, final KiwiGraph graph, final KiwiElement element) {
		super(type, graph, element);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterator<Vertex> iterator() {
		return new KiwiVertexIterator(graph);
	}

}
