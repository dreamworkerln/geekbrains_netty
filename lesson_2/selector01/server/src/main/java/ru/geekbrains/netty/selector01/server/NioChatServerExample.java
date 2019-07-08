package ru.geekbrains.netty.selector01.server;

import ru.geekbrains.netty.selector01.server.ConnectionList;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

public class NioChatServerExample implements Runnable {

    private static final int ROTTEN_LATENCY = 1; //sec

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    //private ByteBuffer buf = ByteBuffer.allocate(256);
    //private int acceptedClientIndex = 1;
    private final ByteBuffer welcomeBuf = ByteBuffer.wrap("Добро пожаловать в чат!\n".getBytes());

    // Non-negative AtomicInteger incrementator
    private static IntUnaryOperator AtomicNonNegativeIntIncrementator = (i) -> i == Integer.MAX_VALUE ? 0 : i + 1;
    // connection id generator
    private static final AtomicInteger connectionIdGen =  new AtomicInteger();

    private ConnectionList connectionList = new ConnectionList();

    NioChatServerExample() throws IOException {

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress("127.0.0.1", 8189));
        serverSocketChannel.configureBlocking(false);

        selector = Selector.open();
        SelectionKey key = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        // start rottening old connections
        scheduleDeleteRottenConnections();

    }

    @Override
    public void run() {
        try {
            System.out.println("Сервер запущен (Порт: 8189)");
            Iterator<SelectionKey> it;
            SelectionKey key;
            // while true
            while (serverSocketChannel.isOpen()) {

                selector.select();
                it = selector.selectedKeys().iterator();

                while (it.hasNext()) {

                    key = it.next();

                    if (key.isAcceptable())
                        handleAccept(key);

                    if (key.isReadable())
                        handleRead(key);

                    it.remove();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    

    private void handleAccept(SelectionKey key) throws IOException {


        ServerSocketChannel serverSocket = (ServerSocketChannel)key.channel();
        System.out.println(serverSocket.getLocalAddress());

        SocketChannel client = serverSocket.accept();
        System.out.println(client.getRemoteAddress());

        int id = connectionIdGen.getAndUpdate(AtomicNonNegativeIntIncrementator);
        //String clientName = "Клиент #" + connectionIdGen.getAndUpdate(AtomicNonNegativeIntIncrementator);
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ, id);
        client.write(welcomeBuf);
        welcomeBuf.rewind();
        System.out.println("Подключился новый клиент #" + id);

        connectionList.add(id, client);
    }

    

    private void handleRead(SelectionKey key) throws IOException {

        SocketChannel client = (SocketChannel) key.channel();
        StringBuilder sb = new StringBuilder();
        int id = (int)key.attachment();

        ByteBuffer buffer = connectionList.get(id).getBuffer();
        int read;

        // read >  0  - readied some data
        // read =  0  - no data available
        // read = -1  - connection closed

        while ((read = client.read(buffer)) > 0) {
            buffer.flip();

            //sb.append(new String(buffer.array(), StandardCharsets.UTF_8).trim());
            byte[] bytes = new byte[buffer.limit()];
            buffer.get(bytes);
            sb.append(new String(bytes, StandardCharsets.UTF_8).trim());

            buffer.clear();
        }
        String msg;

        // Remote endpoint close connection
        if (read < 0) {
            msg = key.attachment() + " покинул чат\n";
            client.close();
            connectionList.remove(id);

        }
        // Remote endpoint transmit some data:
        else {
            msg = key.attachment() + ": " + sb.toString()+"\n";
            connectionList.update(id);// refresh client TTL
        }

        System.out.print(msg);

        broadcastMessage(msg);
    }




    private void broadcastMessage(String msg) throws IOException {

        ByteBuffer msgBuf = ByteBuffer.wrap(msg.getBytes());

        for (SelectionKey key : selector.keys()) {

            // filter only valid (socket not closed)
            // and SocketChannel client keys
            //
            // В selector лежат как ServerSocketChannel (OP_ACCEPT)
            // так и SocketChannel (OP_READ) ключи

            if (key.isValid() &&
                key.channel() instanceof SocketChannel) {

                SocketChannel client = (SocketChannel)key.channel();
                client.write(msgBuf);
                msgBuf.rewind();
            }
        }
    }


    // Schedule rottening old connections
    private void scheduleDeleteRottenConnections() {

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        Thread t = Executors.defaultThreadFactory().newThread(r);
                        t.setDaemon(true);
                        return t;
                    }
                });
        service.scheduleAtFixedRate(
                () -> connectionList.removeRotten(), ROTTEN_LATENCY, ROTTEN_LATENCY, TimeUnit.SECONDS);
    }



    public static void main(String[] args) throws IOException {

        Thread t = new Thread(new NioChatServerExample());
        t.setDaemon(false);
        t.start();
//
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ignore) {}
    }






}