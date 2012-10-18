package com.kv.kiwi.bluekiwi.utils;

public class KiwiElementType {
    public static enum TYPE {
        KIWI_ELEMENT_VERTEX,
        KIWI_ELEMENT_EDGE,
        KIWI_ELEMENT_EDGES_IN,
        KIWI_ELEMENT_EDGES_OUT
        //KIWI_ELEMENT_VERTEX_IN,        only one in/out vertex per edge
        //KIWI_ELEMENT_VERTEX_OUT        no need for an iterator
    }
}