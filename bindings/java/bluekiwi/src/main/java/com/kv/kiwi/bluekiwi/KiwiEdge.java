package com.kv.kiwi.bluekiwi;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;

public class KiwiEdge extends KiwiElement implements Edge {
	private String label = null;
    private KiwiVertex in, out;

	public KiwiEdge(KiwiGraph db, Long id) {
		super(db, id);
	}

	public KiwiEdge(KiwiVertex inVertex, KiwiVertex outVertex, String label, KiwiGraph db) {
		super(db, db.nextEdgeId());
		
        this.in = inVertex;
        this.out = outVertex;
        this.label = label;
        
        
	}

	@Override
	public Vertex getInVertex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public Vertex getOutVertex() {
		// TODO Auto-generated method stub
		return null;
	}

}
