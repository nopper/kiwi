package com.kv.kiwi.bluekiwi;

import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Element;

public abstract class KiwiElement implements Element {
	public Map<String,String> properties;
	protected KiwiGraph db;
	protected Long id;
	
	public KiwiElement(KiwiGraph db, Long id) {
		this.db = db;
		this.id = id;
	}
	
	protected byte[] getRawValue() {
		byte[] key;
		
		if (this instanceof KiwiVertex)
			key = "V".concat(String.valueOf(id)).getBytes();
		else
			key = "E".concat(String.valueOf(id)).getBytes();
		
		return db.getDatabase().get(key, key.length);
	}

	@Override
	public Object getId() {
		return id;
	}
    
	public int hashCode() {
        return this.getId().hashCode();
    }
	
    public boolean equals(Object object) {
        return (this.getClass().equals(object.getClass()) && this.getId().equals(((Element) object).getId()));
    }
	
	@Override
	public Object getProperty(String key) {
		return properties.get(key);
	}

	@Override
	public Set<String> getPropertyKeys() {
		return properties.keySet();
	}

	@Override
	public Object removeProperty(String key) {
		Object old = getProperty(key);
		properties.remove(key);
		save();
		return old;
	}

	abstract void save();

	@Override
	public void setProperty(String key, Object value) {
		properties.put(key, value.toString());
		save();
	}

	public void remove() {
		byte[] key;
		
		if (this instanceof KiwiVertex)
			key = "V".concat(String.valueOf(id)).getBytes();
		else
			key = "E".concat(String.valueOf(id)).getBytes();
		
		db.getDatabase().remove(key, key.length);
	}
	

}
