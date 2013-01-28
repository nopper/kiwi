package com.kv.kiwi.bench;

import java.io.*;
import java.util.*;
import java.math.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class App
{
    public static void main( String[] args )
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(args[0]);
        List<Long> scores = new ArrayList<Long>();

        try{
            Scanner sc = new Scanner(new File(args[1]));
            while(sc.hasNextLong())
            {
                scores.add(sc.nextLong());
            }
            sc.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        System.out.println("There are " + scores.size() + " elements");

        for (Long id: scores)
        {
            final Node node = graphDb.getNodeById(id);
            String name = (String)node.getProperty("name");

            List<Node> neighbors = new ArrayList<Node>();

            for (Relationship rel: node.getRelationships(Direction.OUTGOING))
            {
                neighbors.add(rel.getEndNode());
            }

            System.out.println("Node with ID " + id + " has name " + name + " and " + neighbors.size() + " connections");
        }

        graphDb.shutdown();
    }
}
