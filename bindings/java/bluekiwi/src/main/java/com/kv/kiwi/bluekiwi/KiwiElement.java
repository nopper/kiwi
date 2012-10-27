package com.kv.kiwi.bluekiwi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.kv.kiwi.DBIterator;
import com.kv.kiwi.bluekiwi.serialize.MsgPack;
import com.kv.kiwi.bluekiwi.utils.Utils;
import com.tinkerpop.blueprints.Element;

public abstract class KiwiElement implements Element {
    public Map<String, Object> properties;
    protected KiwiGraph db;
    protected Long id;
    protected String idString;
    protected String directory;

    public KiwiElement(KiwiGraph db, Long id) {
        this.db = db;
        this.id = id;
        this.idString = String.valueOf(id);
        this.properties = new HashMap<String, Object>();

        if (this instanceof KiwiVertex)
            this.directory = "V/";
        else
            this.directory = "E/";
    }

    public String pathFor(String key) {
        if (key == null)
            return directory.toLowerCase().concat(idString);
        else
            return directory.concat(idString.concat("/".concat(key)));
    }

    public String keyFrom(String path) {
        int pos = path.lastIndexOf("/");
        if (pos > 0 && pos < path.length())
            return path.substring(pos + 1);
        return null;
    }

    @Override
    public Object getId() {
        return id;
    }

    public int hashCode() {
        return this.getId().hashCode();
    }

    public boolean equals(Object object) {
        return (this.getClass().equals(object.getClass()) && this.getId()
                .equals(((Element) object).getId()));
    }

    @Override
    public Object getProperty(String key) {
        if (properties.containsKey(key))
            return properties.get(key);

        try {
            // Assuming that the ID of VERTEX and EDGES cannot collide
            byte[] prop = pathFor(key).getBytes();
            byte[] bytes = db.getDatabase().get(prop, prop.length);
            Object value = null;

            if (bytes != null) {
                value = MsgPack.unpack(bytes);

                if (value instanceof byte[])
                    value = new String((byte[]) value);

                if (key.equals("outE") || key.equals("inE"))
                    value = Utils.uncompressList((List<Long>) value);
            } else {
                if (key.equals("outE") || key.equals("inE"))
                    value = new ArrayList<Long>();
            }

            if (value != null)
                properties.put(key, value);

            return value;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        String prefix = pathFor("");
        DBIterator iter = new DBIterator(db.getDatabase(), prefix.getBytes());

        while (iter.isValid()) {
            if (!iter.key().startsWith(prefix))
                break;

            String key = keyFrom(iter.key());

            try {
                Object value = MsgPack.unpack(iter.rawValue());

                if (key.equals("outE") || key.equals("inE"))
                    value = Utils.uncompressList((List<Long>) value);

                if (value instanceof byte[])
                    value = (Object) new String((byte[]) value);

                properties.put(key, value);
            } catch (Exception e) {
                System.out.println("Unable to load property " + iter.key());
                e.printStackTrace();
            }

            iter.next();
        }

        return properties.keySet();
    }

    @Override
    public Object removeProperty(String key) {
        Object old = getProperty(key);
        properties.remove(key);
        db.getDatabase().remove(pathFor(key));
        return old;
    }

    @Override
    public void setProperty(String key, Object value) {
        properties.put(key, value);

        try {

            byte[] k = pathFor(key).getBytes();
            byte[] v;

            if (key.equals("outE") || key.equals("inE"))
                v = MsgPack.pack(Utils.compressList((List<Long>) value));
            else
                v = MsgPack.pack(value);

            db.getDatabase().add(k, k.length, v, v.length);
        } catch (Exception e) {
            System.out.println("Unable to save property " + key);
            e.printStackTrace();
        }
    }

    public void remove() {
        // TODO: remove also the properties
        db.getDatabase().remove(pathFor(null));
    }

}
