package com.kv.kiwi.bluekiwi;

import java.io.IOException;
import java.util.*;

import org.apache.commons.collections.*;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.collections.iterators.TransformIterator;

import com.kv.kiwi.bluekiwi.utils.Utils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultQuery;

public class KiwiVertex extends KiwiElement implements Vertex {
	public Map<String, List<Long>> inE;
	public Map<String, List<Long>> outE;
	public Map<String, List<Long>> inV;
	public Map<String, List<Long>> outV;

	public KiwiVertex(KiwiGraph db) {
		super(db, db.nextVertexId());

		properties = new HashMap<String, String>();
		inE = new HashMap<String, List<Long>>();
		outE = new HashMap<String, List<Long>>();
		inV = new HashMap<String, List<Long>>();
		outV = new HashMap<String, List<Long>>();
		
		save();
	}

	public KiwiVertex(KiwiGraph db, Long id) {
		super(db, id);
		
		try {
			Utils.loadVertex(getRawValue(), this);
		} catch (Exception e) {
			System.err.println("Unable to load vertex with ".concat(String.valueOf(id)));
			e.printStackTrace();
		}
	}

	public KiwiVertex(KiwiGraph db, byte[] key, byte[] value) {
		super(db, Long.parseLong(new String(key).substring(1)));

		try {
			Utils.loadVertex(value, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void save() {
		try {
			if (id == 8)
				System.err.println("VERTEX 8 is present");
			
			byte[] key = "V".concat(String.valueOf(id)).getBytes();
			byte[] value = Utils.serialize(this);
			
			if (id == 8)
				System.err.println("Key: " + new String(key) + " Value: " + new String(value));

			// Save into database
			db.getDatabase().add(key, key.length, value, value.length);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		return new EdgeIterable(this, direction, labels);
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		return new VertexIterable(this, direction, labels);
	}

	@Override
	public Query query() {
		return new DefaultQuery(this);
	}
	
	public class VertexIdTransformer implements Transformer {
		public KiwiGraph database;
		
		public VertexIdTransformer(KiwiGraph database) {
			this.database = database;
		}
		
		@Override
		public Object transform(Object id) {
			return new KiwiVertex(database, (Long)id);
		}
	}
	
	public class EdgeIdTransformer implements Transformer {
		public KiwiGraph database;
		
		public EdgeIdTransformer(KiwiGraph database) {
			this.database = database;
		}
		
		@Override
		public Object transform(Object id) {
			return new KiwiEdge(database, (Long)id);
		}
	}
	
	public class EdgeIterable implements Iterable<Edge> {
		public String[] labels;
		public KiwiVertex vertex;
		public Direction direction;
		public List<Map<String, List<Long>>> vertices;

		public EdgeIterable(KiwiVertex vertex, Direction direction, String[] labels) {
			this.vertex = vertex;
			this.direction = direction;
			this.labels = labels;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Iterator<Edge> iterator() {
			IteratorChain chain = new IteratorChain();
			TransformIterator trans = new TransformIterator(chain);
			trans.setTransformer(new EdgeIdTransformer(vertex.db));
			
			
			if (labels.length == 0)
			{
				if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
					for (String k: vertex.inE.keySet())
						chain.addIterator(vertex.inE.get(k).iterator());
				}
				
				if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
					for (String k: vertex.outE.keySet())
						chain.addIterator(vertex.outE.get(k).iterator());
				}
			}
			else
			{
				if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
					for (String k: labels)
					{
						List<Long> list = vertex.inE.get(k);
						if (list != null)
							chain.addIterator(list.iterator());
					}
				}
				
				if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
					for (String k: labels)
					{
						List<Long> list = vertex.outE.get(k);
						if (list != null)
							chain.addIterator(list.iterator());
					}
				}
			}
			
			return trans;
		}
	}
	
	public class VertexIterable implements Iterable<Vertex> {
		public String[] labels;
		public KiwiVertex vertex;
		public Direction direction;
		public List<Map<String, List<Long>>> vertices;

		public VertexIterable(KiwiVertex vertex, Direction direction, String[] labels) {
			this.vertex = vertex;
			this.direction = direction;
			this.labels = labels;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Iterator<Vertex> iterator() {
			IteratorChain chain = new IteratorChain();
			TransformIterator trans = new TransformIterator(chain);
			trans.setTransformer(new VertexIdTransformer(vertex.db));
			
			
			if (labels.length == 0)
			{
				if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
					for (String k: vertex.inV.keySet())
						chain.addIterator(vertex.inV.get(k).iterator());
				}
				
				if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
					for (String k: vertex.outV.keySet())
						chain.addIterator(vertex.outV.get(k).iterator());
				}
			}
			else
			{
				if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
					for (String k: labels)
					{
						List<Long> list = vertex.inV.get(k);
						if (list != null)
							chain.addIterator(list.iterator());
					}
				}
				
				if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
					for (String k: labels)
					{
						List<Long> list = vertex.outV.get(k);
						if (list != null)
							chain.addIterator(list.iterator());
					}
				}
			}
			
			return trans;
		}
	}

	public void addInEdge(Edge edge) {
		List<Long> lst = inE.get(edge.getLabel());
		
		if (lst == null)
			lst = new ArrayList<Long>();
		
		lst.add((Long)edge.getId());
		inE.put(edge.getLabel(), lst);
	}

	public void addOutEdge(Edge edge) {
		List<Long> lst = outE.get(edge.getLabel());
		
		if (lst == null)
			lst = new ArrayList<Long>();
		
		lst.add((Long)edge.getId());
		outE.put(edge.getLabel(), lst);
	}

	public void addOutVertex(String label, Vertex outVertex) {
		List<Long> lst = outV.get(label);
		
		if (lst == null)
			lst = new ArrayList<Long>();
		
		lst.add((Long)outVertex.getId());
		outV.put(label, lst);
	}
	
	public void addInVertex(String label, Vertex outVertex) {
		List<Long> lst = inV.get(label);
		
		if (lst == null)
			lst = new ArrayList<Long>();
		
		lst.add((Long)outVertex.getId());
		inV.put(label, lst);
	}
	
	public String toString() {
		return new String("v[").concat(String.valueOf(id)).concat("]");
	}
}
