package com.kv.kiwi.bluekiwi.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.kv.kiwi.DB;
import com.kv.kiwi.DBIterator;
import com.kv.kiwi.bluekiwi.KiwiEdge;
import com.kv.kiwi.bluekiwi.KiwiElement;
import com.kv.kiwi.bluekiwi.KiwiGraph;
import com.kv.kiwi.bluekiwi.KiwiVertex;

@SuppressWarnings("rawtypes")
public class KiwiElementIterator implements Iterator {
	protected DB database;
	protected DBIterator iter;
	protected KiwiGraph graph;
	protected KiwiElement element;
	protected KiwiElementType.TYPE type;
	
	private byte[] currentKey;
	private byte prefix;
	
	public KiwiElementIterator(final KiwiElementType.TYPE type, final KiwiGraph graph, final KiwiElement element) {
		this.type = type;
		this.graph = graph;
		this.database = graph.getDatabase();
		this.element = element;
		
		if (type.equals(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX))
		{
			prefix = 'V';
			iter = new DBIterator(database, new byte[] {'V'});
		}
		else
		{
			prefix = 'E';
			iter = new DBIterator(database, new byte[] {'E'});
		}
	}
	
	@Override
	public boolean hasNext() {
		boolean valid = iter.isValid();
		
		if (valid)
		{
			byte[] k = iter.rawKey();
			
			if (k != null && k.length > 0 && k[0] == prefix)
				return true;
			
			return false;
		}
		else
			return false;
	}

	@Override
	public Object next() {
		if (!iter.isValid())
			throw new NoSuchElementException();
		
		currentKey = iter.rawKey();
		byte[] currentValue = iter.rawValue();
		iter.next();
		
		if (currentKey != null && currentKey.length > 0 && currentKey[0] == prefix)
		{
			try {
				if (type.equals(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX))
					return new KiwiVertex(graph, currentKey, currentValue);
				else
					return new KiwiEdge(graph, currentKey, currentValue);
				
			} catch (Exception exc) {
				throw new NoSuchElementException();
			}
		}
		
		throw new NoSuchElementException();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
