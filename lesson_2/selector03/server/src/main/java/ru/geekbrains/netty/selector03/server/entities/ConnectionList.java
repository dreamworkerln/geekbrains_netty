package ru.geekbrains.netty.selector03.server.entities;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;


// ByteBuffer cache for clients
public class ConnectionList implements Iterable<Map.Entry<Integer, Connection>>{

    private static final int ROTTEN_INTERVAL = 100000000; // sec


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


            connTimeList.remove(connection.getTime());

            Instant now = Instant.now();
            connection.setTime(now);

            connTimeList.put(now, id);
        }
    }

    public void remove(int id) {

        Connection connection = connList.get(id);

        if (connection != null) {

            System.out.println("Removing connection #" + id);
            connTimeList.remove(connection.getTime());

            connection.close();
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


        }
    }





    @Override
    public Iterator<Map.Entry<Integer,Connection>> iterator() {

        return connList.entrySet().iterator();
    }

}
