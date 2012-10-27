package com.kv.kiwi.bluekiwi.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Utils {
    public static Long getLong(Object obj) throws IllegalArgumentException {
        Long rv;

        if ((obj.getClass() == Integer.class) || (obj.getClass() == Long.class)
                || (obj.getClass() == Double.class)) {
            rv = Long.parseLong(obj.toString());
        } else if ((obj.getClass() == int.class)
                || (obj.getClass() == long.class)
                || (obj.getClass() == double.class)) {
            rv = (Long) obj;
        } else if (obj.getClass() == String.class) {
            rv = Long.parseLong(obj.toString());
        } else {
            throw new IllegalArgumentException("getLong: type "
                    + obj.getClass() + " = \"" + obj.toString()
                    + "\" unaccounted for");
        }

        return rv;
    }

    public static List<Long> uncompressList(List<Long> lst) {
        long add = 0;
        for (int i = 0; i < lst.size(); i++) {
            add += getLong(lst.get(i));
            lst.set(i, add);
        }
        return lst;
    }

    public static List<Long> compressList(List<Long> value) {
        if (value.size() == 1)
            return value;

        Collections.sort(value);

        List<Long> comp = new ArrayList<Long>(value.size());
        comp.add(value.get(0));

        for (int i = 1; i < value.size(); i++) {
            comp.add(value.get(i) - value.get(i - 1));
        }

        return comp;
    }

    public static String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder("0x");
        for (byte b : data) {
            if (b >= 0 && b < 0x10)
                sb.append("0");
            sb.append(Integer.toString(b & 0xff, 16));
        }
        return sb.toString();
    }

}
