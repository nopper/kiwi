package com.kv.kiwi.bluekiwi;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;

public class KiwiVertex extends KiwiElement implements Vertex {
	
	public KiwiVertex(KiwiGraph db) {
		super(db, db.nextVertexId());
	}
	
	public KiwiVertex(KiwiGraph db, Long id) {
        super(db, id);
    }

	@Override
	public Iterable<Edge> getInEdges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Edge> getOutEdges() {
		// TODO Auto-generated method stub
		return null;
	}
}
