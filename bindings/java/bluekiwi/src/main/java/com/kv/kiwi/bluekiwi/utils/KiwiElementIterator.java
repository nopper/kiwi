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
    protected KiwiElementType.TYPE type;

    protected String key;
    protected Object value;

    private byte[] currentKey;
    private byte prefix;

    public KiwiElementIterator(final KiwiElementType.TYPE type,
            final KiwiGraph graph, String key, Object value) {
        this.type = type;
        this.graph = graph;
        this.database = graph.getDatabase();
        this.key = key;
        this.value = value;

        if (type.equals(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX)) {
            prefix = 'v';
            iter = new DBIterator(database, new byte[] { 'v', '/' });
        } else {
            prefix = 'e';
            iter = new DBIterator(database, new byte[] { 'e', '/' });
        }
    }

    @Override
    public boolean hasNext() {
        boolean valid = iter.isValid();

        if (valid) {
            byte[] k = iter.rawKey();

            if (k != null && k.length > 0 && k[0] == prefix)
                return true;

            return false;
        } else
            return false;
    }

    @Override
    public Object next() {

        while (iter.isValid()) {
            currentKey = iter.rawKey();
            byte[] currentValue = iter.rawValue();
            iter.next();

            if (currentKey != null && currentKey.length > 0
                    && currentKey[0] == prefix) {
                try {
                    KiwiElement ret;

                    if (type.equals(KiwiElementType.TYPE.KIWI_ELEMENT_VERTEX))
                        ret = KiwiVertex.createFrom(graph, currentKey,
                                currentValue);
                    else
                        ret = KiwiEdge.createFrom(graph, currentKey,
                                currentValue);

                    if (key != null) {
                        Object val = ret.getProperty(key);

                        System.out.println("Checking that property " + key
                                + " is set to " + value);
                        System.out.println("Actual value is " + val);

                        if (val != null && val.equals(value))
                            return ret;
                    } else
                        return ret;

                } catch (Exception exc) {
                    exc.printStackTrace();
                    throw new NoSuchElementException();
                }
            } else
                throw new NoSuchElementException();
        }

        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
