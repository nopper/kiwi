package com.kv.kiwi.bluekiwi;

import java.util.Set;

import com.tinkerpop.blueprints.pgm.Element;

public class KiwiElement implements Element {
	private KiwiGraph db;
	private Long id;
	
	public KiwiElement(KiwiGraph db, Long id) {
		this.db = db;
		this.id = id;
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
	public Object getProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getPropertyKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object removeProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		
	}

	public void remove() {
		System.out.println("Removing ".concat(this.toString()));
	}
	

}
