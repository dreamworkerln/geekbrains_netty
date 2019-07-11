package ru.geekbrains.netty.selector01.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


// ByteBuffer cache for clients
public class ConnectionList implements Iterable<Map.Entry<Integer, ConnectionList.Connection>>{

    private static final int BUFFER_SIZE = 3;
    private static final int ROTTEN_INTERVAL = 10000; // sec

    public static class Connection {

        private SelectionKey key;
        private SocketChannel channel;
        private ByteBuffer readBuffer;
        private ByteBuffer writeBuffer; //ToDo: Тут надо городить очередь сообщений в случае чата
        private Instant time;
        private ByteBuffer data;        // emulating data(file) needed to be transferred to client
        private RandomAccessFile file; // not implemented

        private ByteArrayOutputStream bufferStream;


        public Connection(SelectionKey key, Instant time) {

            this.key = key;
            this.channel = (SocketChannel)key.channel();
            this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            this.writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            this.bufferStream = new ByteArrayOutputStream();
            this.time = time;
            this.data = null;
        }

        public ByteBuffer getReadBuffer() {
            return readBuffer;
        }

        public ByteBuffer getWriteBuffer() {
            return writeBuffer;
        }

        public ByteBuffer getData() {
            return data;
        }

        public void setData(ByteBuffer data) {
            this.data = data;
        }

        public SelectionKey getKey() {
            return key;
        }

        public SocketChannel getChannel() {
            return channel;
        }

        public ByteArrayOutputStream getBufferStream() {
            return bufferStream;
        }
    }

    private NavigableMap<Integer, Connection> connList = new ConcurrentSkipListMap<>();
    private NavigableMap<Instant, Integer> connTimeList = new ConcurrentSkipListMap<>();

    public void add(SelectionKey key) {

        Instant now = Instant.now();
        Connection connection = new Connection(key, now);

        int id = (int)key.attachment();
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

        //System.out.println("Removing rotten comnnections ...");

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



    @Override
    public Iterator<Map.Entry<Integer,ConnectionList.Connection>> iterator() {

        return connList.entrySet().iterator();
    }

}
