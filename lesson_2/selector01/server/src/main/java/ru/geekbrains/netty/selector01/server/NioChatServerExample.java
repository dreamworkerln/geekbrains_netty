package ru.geekbrains.netty.selector01.server;

import ru.geekbrains.netty.selector01.server.jobpool.BlockingJobPool;
import ru.geekbrains.netty.selector01.server.utils.LibUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;


// https://www.programering.com/a/MTN1MDMwATk.html
// https://www.ibm.com/developerworks/cn/java/l-niosvr/ => google-translate from china

// SelectionKey.isWritable() - protect socket from flooding
// https://stackoverflow.com/questions/11360374/when-a-selectionkey-turns-writable-in-java-nio

public class NioChatServerExample implements Runnable {

    private static final int ROTTEN_LATENCY = 1; //sec

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    //private ByteBuffer buf = ByteBuffer.allocate(256);
    //private int acceptedClientIndex = 1;
    private final String welcomeString = "Добро пожаловать в чать!\n";

    // Non-negative AtomicInteger incrementator
    private static IntUnaryOperator AtomicNonNegativeIntIncrementator = (i) -> i == Integer.MAX_VALUE ? 0 : i + 1;
    // connection id generator
    private static final AtomicInteger connectionIdGen =  new AtomicInteger();

    private ConnectionList connectionList = new ConnectionList();

    private BlockingJobPool<Void> jobPool =  new BlockingJobPool<>(3, this::onDone);

    NioChatServerExample() throws IOException {

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress("127.0.0.1", 8000));
        serverSocketChannel.configureBlocking(false);

        selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        // start rottening old connections
        scheduleDeleteRottenConnections();

    }


    public void onDone(Void v) {
        System.out.println("Done");
    }

    @Override
    public void run() {

        try {

            System.out.println("Серверо запущено (Порт: 8000)");
            Iterator<SelectionKey> it;
            SelectionKey key;
            // while true
            while (serverSocketChannel.isOpen()) {

                selector.select();
                it = selector.selectedKeys().iterator();

                System.out.println("SELECTING: " + selector.selectedKeys().size());

                while (it.hasNext()) {

                    key = it.next();

                    System.out.println("KEY INTERESTS: " + key.interestOps());
                    System.out.println("KEY READY    : " + key.readyOps());


                    it.remove();

                    // skip invalid keys (disconnected channels)
                    if (!key.isValid())
                        continue;

                    if (key.isAcceptable()) {
                        handleAccept(key);
                    }



                    // Интерес на запись выставляется отдельно
                    // вручную при желании что-либо передать
                    // либо если затопился удаленный сокет и отправка не удалась

                    if (key.isWritable()) {

                        removeInterest(key, SelectionKey.OP_WRITE);

                        // Пишем в отдельном потоке
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {
                            handleWrite(finalKey);
                            return null;
                        });

                    }


                    if (key.isReadable()) {

                        // Чтобы не бегать бесконечно в цикле select
                        // Пока потоки из пула читают из сокетов
                        // Когда они дочитают они сами поднимут флаг OP_READ для key
                        removeInterest(key, SelectionKey.OP_READ);

                        // Читаем в отдельном потоке
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {
                            handleRead(finalKey);
                            return null;
                        });
                    }





//                    SelectionKey finalKey = key;
//                    jobPool.add(() -> {
//
//                        if (finalKey.isAcceptable()) {
//                            handleAccept(finalKey);
//                        }
//
//                        if (finalKey.isReadable()) {
//                            handleRead(finalKey);
//                        }
//                        return null;
//                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void handleAccept(SelectionKey key) {


        try {

            System.out.println("handleAccept");

            //System.out.println(Thread.currentThread().toString());

            ServerSocketChannel serverSocket = (ServerSocketChannel)key.channel();
            System.out.println(serverSocket.getLocalAddress());

            SocketChannel client = serverSocket.accept();
            System.out.println(client.getRemoteAddress());

            int id = connectionIdGen.getAndUpdate(AtomicNonNegativeIntIncrementator);
            //String clientName = "Клиент #" + connectionIdGen.getAndUpdate(AtomicNonNegativeIntIncrementator);
            client.configureBlocking(false);

            SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ, id);

            connectionList.add(clientKey);


            ByteBuffer welcomeBuf = ByteBuffer.wrap(welcomeString.getBytes());
            writeChannel(clientKey, welcomeBuf);

            System.out.println("Подключился новый клиент #" + id);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }






    private void handleRead(SelectionKey key)  {

        try {

            System.out.println("handleRead");

            SocketChannel client = (SocketChannel) key.channel();
            //StringBuilder sb = new StringBuilder();
            int id = (int)key.attachment();

            ByteBuffer buffer = connectionList.get(id).getReadBuffer();
            int read;

            // read >  0  - readied some data
            // read =  0  - no data available
            // read = -1  - connection closed

            ByteArrayOutputStream bufferStream = connectionList.get(id).getBufferStream();


            while ((read = client.read(buffer)) > 0) {
                buffer.flip();
                byte[] bytes = new byte[buffer.limit()];
                buffer.get(bytes);
                bufferStream.write(bytes);
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

                connectionList.update(id);// refresh client TTL

                msg = new String(bufferStream.toByteArray()).trim();
                bufferStream.reset();


                if (msg.equals("sleep"))  {
                    Thread.sleep(100000000);
                }

                // Echoing  back to single client
                // writeChannel(key, buffer);

                // Возвращаем подписку на флаг чтения новых данных из сокета
                // (Была удалена основным потоком сервера,
                // чтобы не бегать бесконечно в цикле селектора,
                // пока threads из пула не вычитали данные из сокета
                // => и тем самым не опустили флаг о возможности чтения из сокета)
                setInterest(key, SelectionKey.OP_READ);
            }

            System.out.print(msg);

            // broadcasting msg to all clients
            msg = key.attachment() + ": " + msg + "\n";
            ByteBuffer broadBuffer = ByteBuffer.wrap(msg.getBytes());
            jobPool.add(() -> {
                broadcast(broadBuffer);
                return null;
            });



            // Update selector
            selector.wakeup();

            //broadcastMessage(msg);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void handleWrite(SelectionKey key)  {


        try {

            System.out.println("handleWrite");

            SocketChannel client = (SocketChannel) key.channel();
            int id = (int)key.attachment();

            ByteBuffer buffer = connectionList.get(id).getWriteBuffer();
            ByteBuffer data = connectionList.get(id).getData();

            int wrote;

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written // need register(selector, SelectionKey.OP_READ, id);
            // wrote  = -1  - connection closed


            // пишем в сокет, пока есть что передавать
            // и сокет принимает данные (не затопился)
            do {

                buffer.rewind();
                LibUtil.copyBuffer(data, buffer);
                buffer.flip();

            }
            while ((wrote = client.write(buffer)) > 0 &
                   data.remaining() > 0);

            // причем, если не отправилось по сети, то в buffer будет лежать кусок
            // (скопированный из data), который так и не отправился
            //
            //
            // Это все к тому, то нельзя начинать передавать новые данные, пока по сети не передалось
            // текущее сообщение


            // Remote endpoint close connection
            if (wrote < 0) {
                String msg = key.attachment() + " покинул чат\n";
                System.out.print(msg);
                client.close();
                connectionList.remove(id);
            }

            // Флудим сокет данными - не успевает принимать на удаленном конце
            // буффер не отправился целиком
            // Регистрируемся на флаг что удаленный сокет может принимать сообщения
            else if (buffer.remaining() > 0) {

                // Сохранить непереданную часть для следущего цикла передачи
                buffer.compact();

                // Выставляем бит OP_WRITE в 1
                // (подписываемся на флаг готовности сокета отправлять данные)
                setInterest(key, SelectionKey.OP_WRITE);
                // update selector
                selector.wakeup();

            }
            // data.remaining() == 0
            else {
                connectionList.get(id).setData(null);

                // prepare buffer to next transmit
                buffer.clear();

                // Если подписывались на флаг готовности на отправку и успешно отправили
                // Выставляем бит OP_WRITE в 0 (отписываемся)
                removeInterest(key, SelectionKey.OP_WRITE);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Write data to client using handleWrite(..)
     */
    private void writeChannel(SelectionKey key, ByteBuffer data) {

        // Т.к. все асинхронное (несколько потоков)
        // То одному и тому же клиенту могут начать отправлять одновременно нескольуо сообщений -
        // Надо делать очередь сообщений (на отправку) для клиента.
        // (Потом, например, удалять только те сообщения, котороые удалось доставить,
        // получится к-то сетевой мессенджер, да клиент еще в оффлайне может быть, очередь хранить надо)

        int id = (int)key.attachment();

        //ToDo: проверить нет ли текущих данных на отправку
        // и если есть, то просто не отправлять
        // Можно, конечно валить все в сокет, (и больше 3 метров в неблокирующем режиме не залезет)
        // Но если там не принимают, то
        // как буффер в нашей сетевой подсистеме переполнится, то данные начнут теряться (что они и делают)


        if (connectionList.get(id).getData() != null) {
            return;
        }

        data.rewind();
        connectionList.get(id).setData(data);
        setInterest(key, SelectionKey.OP_WRITE);
    }


    // Копии data множатся на сервере по 1 штуке на каждого клиента
    // Можно было бы не множить и защитить доступ к data с помощью критической секции
    // Но что делать если один клиент залипнет (и из-за защиты все остальное тоже подвиснет ?)
    // соответственно толкание не глядя в сокет закончится переполнением буфера сокета
    private void broadcast(ByteBuffer data) {

        try {

            for (Map.Entry<Integer,ConnectionList.Connection> entry : connectionList) {

                SelectionKey key = entry.getValue().getKey();

                if (key.isValid()) {


                    ByteBuffer tmp = ByteBuffer.allocate(data.limit());
                    data.rewind();
                    LibUtil.copyBuffer(data,tmp);


                    writeChannel(key, tmp);
                }
            }

            selector.wakeup();

        } catch (Exception e) {
            e.printStackTrace();
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






    public void setInterest(SelectionKey key, int interest) {

        if ((key.interestOps() & interest) == 0) {
            int current = key.interestOps();
            key.interestOps(current | interest);
        }
    }


    public void removeInterest(SelectionKey key, int interest) {

        if ((key.interestOps() & interest) != 0) {
            int current = key.interestOps();
            key.interestOps(current & ~interest);
        }
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