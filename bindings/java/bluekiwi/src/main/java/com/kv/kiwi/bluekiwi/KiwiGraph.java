package com.kv.kiwi.bluekiwi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.kv.kiwi.DB;
import com.kv.kiwi.bluekiwi.utils.KiwiEdgeIterable;
import com.kv.kiwi.bluekiwi.utils.KiwiVertexIterable;
import com.kv.kiwi.bluekiwi.utils.Utils;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class KiwiGraph implements Graph {
    private DB database;
    private long lastVertexId;
    private long lastEdgeId;

    private static final Features FEATURES = new Features();

    public KiwiGraph(String basedir) {
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
        // database.add("schema:lastVertexId", String.valueOf(lastVertexId));
        // database.add("schema:lastEdgeId", String.valueOf(lastEdgeId));
    }

    @Override
    public Edge addEdge(Object arg0, Vertex out, Vertex in, String label) {
        try {
            KiwiEdge edge = new KiwiEdge(this, Utils.getLong(arg0));
            edge.reset(out, in, label);
            edge.save();

            addMissing((KiwiVertex) out, "outE", edge.id);
            addMissing((KiwiVertex) in, "inE", edge.id);

            return edge;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void addMissing(KiwiVertex v, String property, Long id) {
        Object o = v.getProperty(property);
        System.out.println("Property " + property + " is " + o);
        List<Long> value;

        if (o == null)
            value = new ArrayList<Long>();
        else
            value = (List<Long>) o;

        value.add(id);
        Collections.sort(value);

        v.setProperty(property, value);
    }

    @Override
    public Vertex addVertex(Object arg0) {
        KiwiVertex vertex = new KiwiVertex(this);
        vertex.save();

        return vertex;
    }

    @Override
    public Vertex getVertex(Object obj) {
        try {
            final Vertex vertex = new KiwiVertex(this, Utils.getLong(obj));
            return vertex;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Edge getEdge(Object obj) {
        try {
            final Edge edge = new KiwiEdge(this, Utils.getLong(obj));
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
        return new KiwiEdgeIterable(this, key, value);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return new KiwiVertexIterable(this, key, value);
    }

    @Override
    public void removeEdge(Edge edge) {
        ((KiwiElement) edge).remove();
    }

    @Override
    public void removeVertex(Vertex vertex) {
        ((KiwiElement) vertex).remove();
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
        return new String("vertices[".concat(String.valueOf(lastVertexId)
                .concat("]-edges[").concat(String.valueOf(lastEdgeId))
                .concat("]")));
    }

}