package ru.geekbrains.netty.selector01.server;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


// ByteBuffer cache for clients
public class ConnectionList {

    private static final int BUFFER_SIZE = 3;
    private static final int ROTTEN_INTERVAL = 10; // sec

    public static class Connection {

        private SocketChannel channel;
        private ByteBuffer buffer;
        private Instant time;
        private RandomAccessFile file;


        public Connection(SocketChannel channel, Instant time) {

            this.channel = channel;
            this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
            this.time = time;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }
    }

    private NavigableMap<Integer, Connection> connList = new ConcurrentSkipListMap<>();
    private NavigableMap<Instant, Integer> connTimeList = new ConcurrentSkipListMap<>();

    public void add(int id, SocketChannel channel) {

        Instant now = Instant.now();
        Connection connection = new Connection(channel, now);

        connList.put(id, connection);
        connTimeList.put(now, id);
    }


    public Connection get(int id) {

        return connList.get(id);
    }


    public void update(int id) {

        Connection connection = connList.get(id);

        if (connection != null) {


            connTimeList.remove(connection.time);

            Instant now = Instant.now();
            connection.time = now;

            connTimeList.put(connection.time, id);
        }
    }

    public void remove(int id) {

        Connection connection = connList.get(id);

        if (connection != null) {

            System.out.println("Removing connection #" + id);

            // close socket
            SocketChannel channel = connection.channel;
            if (channel != null &&
                channel.isOpen()) {

                try {
                    channel.close();
                } catch (IOException ignored) {}
            }

            // close file
            RandomAccessFile file = connection.file;
            if (file != null) {
                try {
                    file.close();
                } catch (IOException ignored) {}
            }



            connTimeList.remove(connection.time);
        }
        connList.remove(id);
    }

    /**
     * Удаляет протухшие ключи
     */
    public void removeRotten() {

        System.out.println("Removing rotten comnnections ...");

        Instant label = Instant.now().minusSeconds(ROTTEN_INTERVAL);

        NavigableMap<Instant, Integer> rotten = connTimeList.headMap(label , true);

        Iterator<Map.Entry<Instant, Integer>> it = rotten.entrySet().iterator();

        while(it.hasNext()) {

            Map.Entry<Instant, Integer> entry = it.next();
            int id = entry.getValue();

            // remove from connList
            remove(id);

            // remove from connTimeList
            it.remove();
            //ToDo: попробовать поменять местами с remove(id);
            // Вроде как должно сломаться - удаление в цикле без итератора (внутри remove())


        }
    }

}
