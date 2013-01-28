package com.kv.kiwi.bluekiwi;

import java.io.*;
import java.util.*;
import java.math.*;

import com.tinkerpop.blueprints.*;

public class App
{
    public static void main( String[] args )
    {
        KiwiGraph g = new KiwiGraph(args[0]);
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
            Vertex node = g.getVertex(id);
            String name = (String)node.getProperty("name");

            List<Vertex> neighbors = new ArrayList<Vertex>();

            for (Vertex v: node.getVertices(Direction.OUT))
                neighbors.add(v);

            System.out.println("Node with ID " + id + " has name " + name + " and " + neighbors.size() + " connections");
        }

        g.shutdown();
    }
}
