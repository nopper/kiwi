package com.kv.kiwi.bluekiwi;

import java.io.IOException;

import org.msgpack.MessagePack;

import com.kv.kiwi.DB;
import com.kv.kiwi.bluekiwi.iters.KiwiEdgeIterable;
import com.kv.kiwi.bluekiwi.iters.KiwiVertexIterable;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;

public class KiwiGraph implements Graph {
	private DB database;
	private MessagePack struct;
	private long lastVertexId;
	private long lastEdgeId;

	public KiwiGraph(String basedir) {
		struct = new MessagePack();
		database = new DB(basedir);
		
		readSchema();
	}
	
	@Override
	public void clear() {
		System.out.println("clear() is not implemented");
	}
	
	@Override
	public void shutdown() {
		database.close();
		writeSchema();
	}
	
	public Long nextVertexId() {
		return lastVertexId++;
	}

	public Long nextEdgeId() {
		return lastEdgeId++;
	}
	
	public boolean containsVertex(Long id) {
		return database.get("vertex:".concat(String.valueOf(id))).length() > 0;
	}
	
	public boolean containsEdge(Long id) {
		return database.get("edge:".concat(String.valueOf(id))).length() > 0;
	}

	private void readSchema() {
		lastVertexId = 0;
		lastEdgeId = 0;

		try {
			lastVertexId = Long.parseLong(database.get("schema:lastVertexId"));
			lastEdgeId = Long.parseLong(database.get("schema:lastEdgeId"));
		} catch (NumberFormatException e) {
		}
	}
	
	private void writeSchema() {
		database.add("schema:lastVertexId", String.valueOf(lastVertexId));
		database.add("schema:lastEdgeId", String.valueOf(lastEdgeId));
	}

	@Override
	public Edge addEdge(Object arg0, Vertex inVertex, Vertex outVertex, String s) {
		final Edge edge = new KiwiEdge((KiwiVertex) inVertex, (KiwiVertex) outVertex, s, this);
		return edge;
	}

	@Override
	public Vertex addVertex(Object arg0) {
		final Vertex vertex = new KiwiVertex(this);
		return vertex;
	}

	@Override
	public Vertex getVertex(Object obj) {
		try {
			Long id = getLong(obj);

			if (containsVertex(id)) {
				final Vertex vertex = new KiwiVertex(this, id);
				return vertex;
			}

			return null;
		} catch (Exception e) {
			return null;
		}
	}
	
	@Override
	public Edge getEdge(Object obj) {
		try {
			Long id = getLong(obj);

			if (containsEdge(id)) {
				final Edge edge = new KiwiEdge(this, id);
				return edge;
			}

			return null;
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Iterable<Vertex> getVertices() {
		return new KiwiVertexIterable(this);
	}
	
	@Override
	public Iterable<Edge> getEdges() {
		return new KiwiEdgeIterable(this);
	}

	@Override
	public Index getIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeEdge(Edge edge) {
		((KiwiElement)edge).remove();
	}

	@Override
	public void removeVertex(Vertex vertex) {
		((KiwiElement)vertex).remove();
	}

	private final Long getLong(Object obj) throws IllegalArgumentException {
		Long rv;

		if ((obj.getClass() == Integer.class) || (obj.getClass() == Long.class)
				|| (obj.getClass() == Double.class)) {
			rv = Long.parseLong(obj.toString());
		} else if ((obj.getClass() == int.class)
				|| (obj.getClass() == long.class)
				|| (obj.getClass() == double.class)) {
			rv = (Long) obj;
		} else if (obj.getClass() == String.class) {
			rv = Long.parseLong(obj.toString());
		} else {
			throw new IllegalArgumentException("getLong: type "
					+ obj.getClass() + " = \"" + obj.toString()
					+ "\" unaccounted for");
		}

		return rv;
	}
}