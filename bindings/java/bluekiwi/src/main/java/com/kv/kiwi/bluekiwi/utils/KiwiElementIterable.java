package com.kv.kiwi.bluekiwi.utils;

import com.kv.kiwi.bluekiwi.KiwiElement;
import com.kv.kiwi.bluekiwi.KiwiGraph;

public class KiwiElementIterable {
	protected KiwiElementType.TYPE type;
	protected KiwiGraph graph;
	
	public KiwiElementIterable(KiwiElementType.TYPE type, KiwiGraph graph, KiwiElement element) {
		this.type = type;
		this.graph = graph;
	}
}
