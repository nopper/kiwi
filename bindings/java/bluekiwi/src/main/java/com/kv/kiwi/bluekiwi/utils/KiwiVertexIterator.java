package com.kv.kiwi.bluekiwi.utils;

import com.kv.kiwi.bluekiwi.KiwiElement;
import com.kv.kiwi.bluekiwi.KiwiGraph;

public class KiwiVertexIterator extends KiwiElementIterator {
	public KiwiVertexIterator(final KiwiGraph graph) {
		this(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX, graph, null);
	}
	
	public KiwiVertexIterator(final KiwiElementType.TYPE type, final KiwiGraph graph, final KiwiElement element) {
		super(type, graph, element);
	}
}
