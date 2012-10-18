package com.kv.kiwi.bluekiwi.utils;

import com.kv.kiwi.bluekiwi.KiwiElement;
import com.kv.kiwi.bluekiwi.KiwiGraph;

public class KiwiEdgeIterator extends KiwiElementIterator {
	public KiwiEdgeIterator(final KiwiGraph graph) {
		this(KiwiElementType.TYPE.KIWI_ELEMENT_EDGE, graph, null);
	}
	
	public KiwiEdgeIterator(final KiwiElementType.TYPE type, final KiwiGraph graph, final KiwiElement element) {
		super(type, graph, element);
	}
}
