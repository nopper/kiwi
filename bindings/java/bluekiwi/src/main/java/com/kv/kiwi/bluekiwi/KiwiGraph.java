package com.kv.kiwi.bluekiwi;

//import org.msgpack.MessagePack;

import com.kv.kiwi.DB;
import com.kv.kiwi.bluekiwi.utils.KiwiEdgeIterable;
import com.kv.kiwi.bluekiwi.utils.KiwiVertexIterable;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Graph;

public class KiwiGraph implements Graph {
	private DB database;
	// private MessagePack struct;
	private long lastVertexId;
	private long lastEdgeId;

	private static final Features FEATURES = new Features();

	public KiwiGraph(String basedir) {
		// struct = new MessagePack();
		database = new DB(basedir);

		readSchema();
	}

	@Override
	public void shutdown() {
		writeSchema();
		database.shutdown();
	}

	public Long nextVertexId() {
		return lastVertexId++;
	}

	public Long nextEdgeId() {
		return lastEdgeId++;
	}

	private void readSchema() {
		lastVertexId = 1;
		lastEdgeId = 1;

		try {
			lastVertexId = Long.parseLong(database.get("schema:lastVertexId"));
			lastEdgeId = Long.parseLong(database.get("schema:lastEdgeId"));
		} catch (Exception exc) {
		}
	}

	private void writeSchema() {
		//database.add("schema:lastVertexId", String.valueOf(lastVertexId));
		//database.add("schema:lastEdgeId", String.valueOf(lastEdgeId));
	}

	@Override
	public Edge addEdge(Object arg0, Vertex src, Vertex dst, String label) {
		((KiwiVertex)src).addOutVertex(label, dst);
		((KiwiVertex)dst).addInVertex(label, src);
		
		final Edge edge = new KiwiEdge((Long)src.getId(), (Long)dst.getId(), label, this);
		
		((KiwiVertex)src).addOutEdge(edge);
		((KiwiVertex)dst).addInEdge(edge);
		
		((KiwiVertex)src).save();
		((KiwiVertex)dst).save();
		
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
			final Vertex vertex = new KiwiVertex(this, getLong(obj));
			return vertex;
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Edge getEdge(Object obj) {
		try {
			final Edge edge = new KiwiEdge(this, getLong(obj));
			return edge;
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Iterable<Edge> getEdges() {
		return new KiwiEdgeIterable(this);
	}

	@Override
	public Iterable<Vertex> getVertices() {
		return new KiwiVertexIterable(this);
	}

	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeEdge(Edge edge) {
		((KiwiElement) edge).remove();
	}

	@Override
	public void removeVertex(Vertex vertex) {
		((KiwiElement) vertex).remove();
	}

	public final Long getLong(Object obj) throws IllegalArgumentException {
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

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	static {
		FEATURES.supportsDuplicateEdges = true;
		FEATURES.supportsSelfLoops = true;
		FEATURES.isPersistent = true;
		FEATURES.supportsVertexIteration = true;
		FEATURES.supportsEdgeIteration = true;
		FEATURES.supportsVertexIndex = false;
		FEATURES.supportsEdgeIndex = false;
		FEATURES.ignoresSuppliedIds = true;
		FEATURES.supportsEdgeRetrieval = true;
		FEATURES.supportsVertexProperties = true;
		FEATURES.supportsEdgeProperties = true;
		FEATURES.supportsTransactions = false;
		FEATURES.supportsIndices = false;

		FEATURES.supportsSerializableObjectProperty = true;
		FEATURES.supportsBooleanProperty = true;
		FEATURES.supportsDoubleProperty = true;
		FEATURES.supportsFloatProperty = true;
		FEATURES.supportsIntegerProperty = true;
		FEATURES.supportsPrimitiveArrayProperty = true;
		FEATURES.supportsUniformListProperty = true;
		FEATURES.supportsMixedListProperty = true;
		FEATURES.supportsLongProperty = true;
		FEATURES.supportsMapProperty = true;
		FEATURES.supportsStringProperty = true;

		FEATURES.isWrapper = true;
		FEATURES.supportsKeyIndices = true;
		FEATURES.supportsVertexKeyIndex = true;
		FEATURES.supportsEdgeKeyIndex = true;
		FEATURES.supportsThreadedTransactions = false;
	}

	public DB getDatabase() {
		return database;
	}
	
	public String toString() {
		return new String("vertices[".concat(String.valueOf(lastVertexId).concat("]-edges[").concat(String.valueOf(lastEdgeId)).concat("]")));
	}

}