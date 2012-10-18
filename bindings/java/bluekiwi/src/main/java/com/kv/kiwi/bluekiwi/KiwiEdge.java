package com.kv.kiwi.bluekiwi;

import java.io.IOException;
import java.util.HashMap;

import com.kv.kiwi.bluekiwi.utils.Utils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class KiwiEdge extends KiwiElement implements Edge {
	public String label = null;
	public long inId = 0;
	public long outId = 0;

	public KiwiEdge(KiwiGraph db, Long id) {
		super(db, id);
		try {
			Utils.loadEdge(getRawValue(), this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public KiwiEdge(KiwiGraph db, byte[] key, byte[] value) {
		super(db, Long.parseLong(new String(key).substring(1)));

		try {
			Utils.loadEdge(value, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public KiwiEdge(long inVertex, long outVertex, String label, KiwiGraph db) {
		super(db, db.nextEdgeId());

		this.inId = inVertex;
		this.outId = outVertex;
		this.label = label;
		
		// Save into database
		properties = new HashMap<String, String>();
		
		save();
	}
	
	public void save() {
		try {
			byte[] key = "E".concat(String.valueOf(id)).getBytes();
			byte[] value = Utils.serialize(this);

			// Save into database
			db.getDatabase().add(key, key.length, value, value.length);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
		if (direction.equals(Direction.BOTH))
			throw new IllegalArgumentException();
		
		if (direction.equals(Direction.OUT))
		{
			return new KiwiVertex(db, outId);
		}
		else
		{
			return new KiwiVertex(db, inId);
		}
	}
	
	public String toString() {
		return new String("e[").concat(String.valueOf(id)).concat("][").concat(String.valueOf(inId)).concat("-").concat(label).concat("->").concat(String.valueOf(outId).concat("]"));
	}

}
