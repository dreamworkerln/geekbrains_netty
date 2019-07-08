package ru.geekbrains.netty.selector00;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

public class NioChatServerExample implements Runnable {

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    //private ByteBuffer buf = ByteBuffer.allocate(256);
    //private int acceptedClientIndex = 1;
    private final ByteBuffer welcomeBuf = ByteBuffer.wrap("Добро пожаловать в чат!\n".getBytes());


    // Non-negative AtomicInteger incrementator
    private static IntUnaryOperator AtomicNonNegativeIntIncrementator = (i) -> i == Integer.MAX_VALUE ? 0 : i + 1;
    // connection id generator
    private static final AtomicInteger connectionIdGen =  new AtomicInteger();

    NioChatServerExample() throws IOException {

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress("127.0.0.1", 8189));
        serverSocketChannel.configureBlocking(false);

        selector = Selector.open();
        SelectionKey key = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        try {
            System.out.println("Сервер запущен (Порт: 8189)");
            Iterator<SelectionKey> iter;
            SelectionKey key;
            // while true
            while (serverSocketChannel.isOpen()) {

                selector.select();
                iter = selector.selectedKeys().iterator();

                while (iter.hasNext()) {

                    key = iter.next();

                    if (key.isAcceptable())
                        handleAccept(key);

                    if (key.isReadable())
                        handleRead(key);

                    iter.remove();
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
        System.out.println(client.getLocalAddress());

        String clientName = "Клиент #" + connectionIdGen.getAndUpdate(AtomicNonNegativeIntIncrementator);
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ, clientName);
        client.write(welcomeBuf);
        welcomeBuf.rewind();
        System.out.println("Подключился новый клиент " + clientName);
    }

    

    private void handleRead(SelectionKey key) throws IOException {

        SocketChannel client = (SocketChannel) key.channel();
        StringBuilder sb = new StringBuilder();

        ByteBuffer buffer = ByteBuffer.allocate(256);
        int read;
        while ((read = client.read(buffer)) > 0) {
            buffer.flip();
            //byte[] bytes = new byte[buffer.limit()];
            //buffer.get(bytes);
            sb.append(new String(buffer.array(), StandardCharsets.UTF_8).trim());

            buffer.compact();
        }
        String msg;
        if (read < 0) {
            msg = key.attachment() + " покинул чат\n";
            client.close();
        } else {
            msg = key.attachment() + ": " + sb.toString()+"\n";
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

    public static void main(String[] args) throws IOException {
        new Thread(new NioChatServerExample()).start();
    }
}