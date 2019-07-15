package ru.geekbrains.netty.selector03.common.entities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

public class Utils {

    public static String channelToString(SeekableByteChannel channel) {

        String result = null;

        try {
            ByteBuffer buffer = ByteBuffer.allocate((int)channel.size());
            channel.position(0);
            channel.read(buffer);
            buffer.flip();

            result = new String( buffer.array(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void StringTochannel(String text, SeekableByteChannel outChannel) {

        SeekableByteChannel result = null;

        try {
            ByteBuffer tmpBuffer = ByteBuffer.wrap(text.getBytes());

            outChannel.position(0);
            outChannel.truncate(tmpBuffer.capacity());
            outChannel.write(tmpBuffer);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


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
