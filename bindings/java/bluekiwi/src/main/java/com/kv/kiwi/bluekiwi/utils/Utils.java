package com.kv.kiwi.bluekiwi.utils;

import static org.msgpack.template.Templates.TLong;
import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.tList;
import static org.msgpack.template.Templates.tMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;
import org.msgpack.unpacker.Unpacker;

import com.kv.kiwi.bluekiwi.KiwiEdge;
import com.kv.kiwi.bluekiwi.KiwiVertex;

public class Utils {
	private static MessagePack msgpack = new MessagePack();
	private static ByteArrayOutputStream out = new ByteArrayOutputStream();
	private static Packer packer = msgpack.createPacker(out);
	
	private static Template<Map<String, String>> ssmapTmpl = tMap(TString, TString);
	private static Template<List<Long>> llstTmpl = tList(TLong);
	private static Template<Map<String, List<Long>>> slimapTmpl = tMap(TString, llstTmpl);
	
	private static void addMap(Map<String, List<Long>> h) throws IOException {
		packer.writeMapBegin(h.size());
		{
			for (Map.Entry<String, List<Long>> e: h.entrySet())
			{
				packer.write(e.getKey());
				packer.write(e.getValue());
			}
		}
		packer.writeMapEnd();
	}
	
	public static byte[] serialize(KiwiVertex v) throws IOException {
		out.reset();
		packer.write(v.properties);
		
		addMap(v.inE);
		addMap(v.outE);
		addMap(v.inV);
		addMap(v.outV);

		return out.toByteArray();
	}
	
	public static byte[] serialize(KiwiEdge e) throws IOException {
		out.reset();
		
		packer.write(e.inId);
		packer.write(e.outId);
		packer.write(e.label);
		packer.write(e.properties);

		return out.toByteArray();
	}
	
	public static void loadVertex(byte[] bytes, KiwiVertex v) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		Unpacker u = msgpack.createUnpacker(in);

		v.properties = u.read(ssmapTmpl);
		v.inE = u.read(slimapTmpl);
		v.outE = u.read(slimapTmpl);
		v.inV = u.read(slimapTmpl);
		v.outV = u.read(slimapTmpl);
	}
	
	public static void loadEdge(byte[] bytes, KiwiEdge e) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		Unpacker u = msgpack.createUnpacker(in);
		
		e.inId = u.read(TLong);
		e.outId = u.read(TLong);
		e.label = u.read(TString);
		e.properties = u.read(ssmapTmpl);
	}
}
