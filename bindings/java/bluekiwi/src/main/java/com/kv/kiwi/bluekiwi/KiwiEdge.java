package com.kv.kiwi.bluekiwi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.kv.kiwi.bluekiwi.serialize.MsgPack;
import com.kv.kiwi.bluekiwi.utils.Utils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class KiwiEdge extends KiwiElement implements Edge {
    public String label = null;
    public KiwiVertex inV = null;
    public KiwiVertex outV = null;

    public KiwiEdge(KiwiGraph db, Long id) {
        super(db, id);
        load();
    }

    public static KiwiEdge createFrom(KiwiGraph db, byte[] currentKey,
            byte[] currentValue) throws IOException {
        long id = Long.parseLong(new String(currentKey).substring(2));
        KiwiEdge e = new KiwiEdge(db, id);

        List<Object> lst = (List<Object>) MsgPack.unpack(currentValue);
        e.inV = new KiwiVertex(db, Utils.getLong(lst.get(0)));
        e.outV = new KiwiVertex(db, Utils.getLong(lst.get(1)));
        e.label = new String((byte[]) lst.get(2));

        return e;
    }

    public void reset(final Vertex out, final Vertex in, String lbl) {
        label = lbl;
        inV = (KiwiVertex) in;
        outV = (KiwiVertex) out;
    }

    private void load() {
        try {
            byte[] k = pathFor(null).getBytes();
            ArrayList<Object> lst = (ArrayList<Object>) MsgPack.unpack(db
                    .getDatabase().get(k, k.length));
            inV = new KiwiVertex(db, Utils.getLong(lst.get(0)));
            outV = new KiwiVertex(db, Utils.getLong(lst.get(1)));
            label = new String((byte[]) lst.get(2));
        } catch (Exception e) {
            System.out.println("Unable to load edge " + getId());
            e.printStackTrace();
        }
    }

    public void save() {
        ArrayList<Object> lst = new ArrayList<Object>();
        lst.add(inV.getId());
        lst.add(outV.getId());
        lst.add(label);
        byte[] key = pathFor(null).getBytes();
        byte[] value = MsgPack.pack(lst);
        db.getDatabase().add(key, key.length, value, value.length);
    }

    @Override
    public String getLabel() {
        if (label == null)
            load();

        return label;
    }

    @Override
    public Vertex getVertex(Direction direction)
            throws IllegalArgumentException {

        if (direction.equals(Direction.BOTH))
            throw new IllegalArgumentException();

        if (inV == null || outV == null)
            load();

        // TODO: if you need a new vertex just tell me
        if (direction.equals(Direction.OUT)) {
            return (Vertex) new KiwiVertex(db, (Long) outV.getId());
        } else {
            return (Vertex) new KiwiVertex(db, (Long) inV.getId());
        }
    }

    public String toString() {
        if (inV == null || outV == null)
            load();

        return new String("e[").concat(idString).concat("][")
                .concat(String.valueOf(outV.getId())).concat("-").concat(label)
                .concat("->").concat(String.valueOf(inV.getId()).concat("]"));
    }

}
