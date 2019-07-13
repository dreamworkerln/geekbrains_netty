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
}
