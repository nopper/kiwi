package com.kv.kiwi.bluekiwi;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.collections.iterators.TransformIterator;

import com.kv.kiwi.bluekiwi.utils.Utils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultQuery;

public class KiwiVertex extends KiwiElement implements Vertex {
    public KiwiVertex(KiwiGraph db) {
        super(db, db.nextVertexId());
    }

    public KiwiVertex(KiwiGraph db, Long id) {
        super(db, id);
    }

    public static KiwiVertex createFrom(KiwiGraph db, byte[] currentKey,
            byte[] currentValue) {
        return new KiwiVertex(db, Utils.getLong(new String(currentKey)
                .substring(2)));
    }

    public void save() {
        db.getDatabase().add(pathFor(null), "");
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
            return new KiwiVertex(database, (Long) id);
        }
    }

    public class EdgeIdTransformer implements Transformer {
        public KiwiGraph database;

        public EdgeIdTransformer(KiwiGraph database) {
            this.database = database;
        }

        @Override
        public Object transform(Object id) {
            return new KiwiEdge(database, Utils.getLong(id));
        }
    }

    public class EdgeToVertexTransformer implements Transformer {
        public KiwiGraph database;
        public Direction direction;

        public EdgeToVertexTransformer(KiwiGraph database, Direction direction) {
            this.database = database;
            this.direction = direction;
        }

        @Override
        public Object transform(Object edge) {
            if (direction.equals(Direction.IN))
                return ((KiwiEdge) edge).getVertex(Direction.OUT);
            else
                return ((KiwiEdge) edge).getVertex(Direction.IN);
        }
    }

    public class EdgeLabelFilter implements Predicate {
        public HashSet<String> labels;

        public EdgeLabelFilter(String[] labels) {
            this.labels = new HashSet<String>(Arrays.asList(labels));
        }

        @Override
        public boolean evaluate(Object arg) {
            return labels.contains(((KiwiEdge) arg).getLabel());
        }
    }

    public class VertexIterable implements Iterable<Vertex> {
        public String[] labels;
        public KiwiVertex vertex;
        public Direction direction;

        public VertexIterable(KiwiVertex vertex, Direction direction,
                String[] labels) {
            this.vertex = vertex;
            this.direction = direction;
            this.labels = labels;
        }

        private TransformIterator transform(List<Long> lst) {
            TransformIterator trans = new TransformIterator();
            trans.setTransformer(new EdgeIdTransformer(vertex.db));
            trans.setIterator(lst.iterator());
            return trans;
        }

        @SuppressWarnings("unchecked")
        public Iterator<Vertex> createIterator(Direction direction) {
            Iterator it;

            if (direction.equals(Direction.IN))
                it = transform((List<Long>) vertex.getProperty("inE"));
            else if (direction.equals(Direction.OUT))
                it = transform((List<Long>) vertex.getProperty("outE"));
            else
                // NOTE: You need to chain two of these to support BOTH
                throw new UnsupportedOperationException();

            if (labels.length == 0)
                return toVertices(it, direction);

            FilterIterator filter = new FilterIterator(it);
            filter.setPredicate(new EdgeLabelFilter(labels));

            return toVertices(filter, direction);
        }

        private Iterator<Vertex> toVertices(Iterator it, Direction direction) {
            TransformIterator trans = new TransformIterator();
            trans.setIterator(it);
            trans.setTransformer(new EdgeToVertexTransformer(vertex.db,
                    direction));
            return trans;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<Vertex> iterator() {
            if (direction.equals(Direction.BOTH)) {
                IteratorChain chain = new IteratorChain();
                chain.addIterator(createIterator(Direction.IN));
                chain.addIterator(createIterator(Direction.OUT));
                return chain;
            }

            return createIterator(direction);
        }
    }

    public class EdgeIterable implements Iterable<Edge> {
        public String[] labels;
        public KiwiVertex vertex;
        public Direction direction;

        public EdgeIterable(KiwiVertex vertex, Direction direction,
                String[] labels) {
            this.vertex = vertex;
            this.direction = direction;
            this.labels = labels;
        }

        private TransformIterator transform(List<Long> lst) {
            TransformIterator trans = new TransformIterator();
            trans.setTransformer(new EdgeIdTransformer(vertex.db));
            trans.setIterator(lst.iterator());
            return trans;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterator<Edge> iterator() {
            Iterator it;

            if (direction.equals(Direction.IN))
                it = transform((List<Long>) vertex.getProperty("inE"));
            else if (direction.equals(Direction.OUT))
                it = transform((List<Long>) vertex.getProperty("outE"));
            else {
                IteratorChain chain = new IteratorChain();
                chain.addIterator(transform((List<Long>) vertex
                        .getProperty("inE")));
                chain.addIterator(transform((List<Long>) vertex
                        .getProperty("outE")));
                it = chain;
            }

            if (labels.length == 0)
                return it;

            FilterIterator filter = new FilterIterator(it);
            filter.setPredicate(new EdgeLabelFilter(labels));

            return filter;
        }
    }

    public String toString() {
        return new String("v[").concat(String.valueOf(id)).concat("]");
    }
}
