package ru.geekbrains.netty.selector03.server.utils;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

public class Utils {

    public static void copyBuffer(ByteBuffer src, ByteBuffer dst) {

        int maxTransfer = Math.min(dst.remaining(), src.remaining());

        // use a duplicated(backed on original) buffer so we don't disrupt the limit of the original buffer
        ByteBuffer tmp = src.duplicate();
        tmp.limit(tmp.position() + maxTransfer);
        dst.put(tmp);

        // now discard the data we've copied from the original source (optional)
        src.position(src.position() + maxTransfer);
    }

    public static boolean isNullOrEmpty(Object object) {

        return object == null || object.getClass() == String.class && ((String)object).trim().isEmpty();
    }


}
