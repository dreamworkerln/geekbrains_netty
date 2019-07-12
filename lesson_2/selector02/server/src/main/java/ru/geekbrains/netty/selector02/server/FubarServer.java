package ru.geekbrains.netty.selector02.server;

import ru.geekbrains.netty.selector02.server.entities.ConnectionList;
import ru.geekbrains.netty.selector02.server.entities.jobpool.BlockingJobPool;
import ru.geekbrains.netty.selector02.server.serverActions.DirectoryReader;
import static ru.geekbrains.netty.selector02.server.utils.Utils.isNullOrEmpty;
import static ru.geekbrains.netty.selector02.server.utils.Utils.copyBuffer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;


// https://www.programering.com/a/MTN1MDMwATk.html
// https://www.ibm.com/developerworks/cn/java/l-niosvr/ => google-translate from china

// SelectionKey.isWritable() - protect socket from flooding
// https://stackoverflow.com/questions/11360374/when-a-selectionkey-turns-writable-in-java-nio

public class FubarServer implements Runnable {

    private static final int ROTTEN_LATENCY = 1; //sec

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    //private ByteBuffer buf = ByteBuffer.allocate(256);
    //private int acceptedClientIndex = 1;
    private final String welcomeString = "Fubar Transfer Protocol server приветствует вас.\n";

    // Non-negative AtomicInteger incrementator
    private static IntUnaryOperator AtomicNonNegativeIntIncrementator = (i) -> i == Integer.MAX_VALUE ? 0 : i + 1;
    // connection id generator
    private static final AtomicInteger connectionIdGen =  new AtomicInteger();

    private ConnectionList connectionList = new ConnectionList();

    private BlockingJobPool<Void> jobPool =  new BlockingJobPool<>(4, this::onDone);

    private static final int PORT_NUMBER = 8000;


    private String dataRoot;

    FubarServer() throws IOException {

        // Будут проблемы с путями
        dataRoot = System.getProperty("user.dir") + "/server/data/";  //(? File.separator)

        Files.createDirectories(Paths.get(dataRoot));

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress("127.0.0.1", PORT_NUMBER));
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

            System.out.println("Серверо запущено (Порт: " + PORT_NUMBER + ")");
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





                    if (key.isReadable()) {

                        // Чтобы не бегать бесконечно в цикле select
                        // Пока потоки из пула читают из сокетов.
                        removeInterest(key, SelectionKey.OP_READ);

                        // Читаем текстовую команду в отдельном потоке
                        // Затем назначаем ее на выполнение
                        // (выполняться будет потом в другом потоке)
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {

                            String command = readText(finalKey);
                            processCommand(finalKey, command);

                            // Возвращаем подписку на флаг чтения новых данных из сокета
                            setInterest(finalKey, SelectionKey.OP_READ);
                            // Будим селектор
                            selector.wakeup();

                            return null;
                        });
                    }



                    // Интерес на запись выставляется отдельно
                    // вручную при желании что-либо передать
                    // либо внутри handleWrite(...) если затопился сокет и отправка не удалась
                    if (key.isWritable()) {

                        removeInterest(key, SelectionKey.OP_WRITE);

                        // Пишем в отдельном потоке
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {
                            handleWrite(finalKey);
                            return null;
                        });

                    }
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
            //System.out.println("LOCAL: " + serverSocket.getLocalAddress());

            SocketChannel client = serverSocket.accept();
            System.out.println("REMOTE ENDPOINT: " + client.getRemoteAddress());

            // Нет свободных потоков - нечем обрабатывать клиента
            if (jobPool.isFull()) {

                System.out.println("No workers - disconnecting");
                client.close();
                return;
            }

            int id = connectionIdGen.getAndUpdate(AtomicNonNegativeIntIncrementator);
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




    /**
     * Читаем из сокета данные, сколько их там накопилось
     * (в буффере сокета) к текущему времени
     * Используется для чтения текстовых комманд небольшой длины
     */
    private String readText(SelectionKey key)  {

        String result = null;

        try {

            SocketChannel client = (SocketChannel) key.channel();
            int id = (int)key.attachment();

            ByteBuffer buffer = connectionList.get(id).getReadBuffer();
            int read;

            // read >  0  - readied some data
            // read =  0  - no data available
            // read = -1  - connection closed

            ByteArrayOutputStream bufferStream = connectionList.get(id).getBufferStream();


            // Читаем данные, доступные в буфере сокета и забиваем на оставшиеся
            // (по идее их не должно быть)
            // Соответственно на другом конце не надо много передавать
            while ((read = client.read(buffer)) > 0) {
                bufferStream.write(buffer.array(), 0, buffer.position());
                buffer.clear();
            }

            // Remote endpoint close connection
            // Если не дочиталось считаем все данные, накопленные в
            // bufferStream бракованными
            if (read < 0) {
                System.out.println(key.attachment() + " покинул чат");
                connectionList.remove(id); // will close channel
            }
            // Remote endpoint transmit some data
            // Что-то прочиталось от клиента
            else {

                // refresh client TTL
                connectionList.update(id);

                result = new String(bufferStream.toByteArray(), StandardCharsets.UTF_8).trim();
                System.out.println("IN: " + result);
            }

            bufferStream.reset();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }


    /**
     * User commands parser and procesor
     */


    private void processCommand(SelectionKey key, String command) {

        String result = null;

        // Bad input
        if (isNullOrEmpty(command)) {
            return;
        }

        String[] parts = command.split(" ");

        switch (parts[0]) {

            case "list":
                Function<String,String> dirNfo = new DirectoryReader();
                result = dirNfo.apply(dataRoot);
                break;

            case "get":

                if (parts.length < 2 ||
                    isNullOrEmpty(parts[1])) {
                    result = "invalid command args";
                }
                else {
                    String filePathString =  ;

                    Path file = Paths.get(dataRoot + parts[1]);
                    Files.exists(file);

                }
                

                break;




            default:
                result = "unknown command";
                break;
        }









        // Ответ клиенту (пока везде текст)
        res = parseCommand(key, msg);

        // Отвечаем обратно клиенту текстом
        ByteBuffer writeBuffer = ByteBuffer.wrap(res.getBytes());
        writeChannel(key, writeBuffer);
    }



    // command router
    private String parseCommand(SelectionKey key, String msg) {

        String result = "";

        // sleeping
        if (msg.equals("sleep"))  {
            try {
                Thread.sleep(100000000);
            } catch (Exception ignore) {}

        }
        // DIR LIST
        else if (msg.equalsIgnoreCase("list")) {

            Function<String,String> dirNfo = new DirectoryReader();
            result = dirNfo.apply(dataRoot);
        }
        // UNKNOWN COMMAND
        else {
            result = key.attachment() + ": " + msg + "\n";
        }
        return result;
    }





    private void handleWrite(SelectionKey key)  {

        try {

            System.out.println("handleWrite");

            SocketChannel client = (SocketChannel) key.channel();
            int id = (int)key.attachment();

            ByteBuffer buffer = connectionList.get(id).getWriteBuffer();
            ByteBuffer data = connectionList.get(id).getData();

            int remainingBefore = data.remaining();

            int wrote;

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written // need register(selector, SelectionKey.OP_WRITE, id);
            // wrote  = -1  - connection closed


            // пишем в сокет, пока есть что передавать
            // и сокет принимает данные (не затопился)
            do {

                buffer.rewind();
                copyBuffer(data, buffer);
                buffer.flip();

            }
            while ((wrote = client.write(buffer)) > 0 &
                   data.remaining() > 0);
            // -------------------------------------------------
            // Если хоть что-то передалось
            if (remainingBefore > data.remaining()) {
                // refresh client TTL
                connectionList.update(id);
            }
            // -------------------------------------------------

            // причем, если не отправилось по сети, то в buffer будет лежать кусок
            // (скопированный из data), который так и не отправился
            //
            // Это все к тому, то нельзя начинать передавать новые данные, пока по сети не передалось
            // текущее сообщение

            // -------------------------------------------------------------------------
            // Remote endpoint close connection
            if (wrote < 0) {
                System.out.println(key.attachment() + " отключился");
                client.close();
                connectionList.remove(id);
            }
            // -------------------------------------------------------------------------
            // Флудим сокет данными - он не успевает принимать на удаленном конце
            // буффер не отправился целиком
            // Регистрируемся на флаг что удаленный сокет может принимать сообщения
            else if (buffer.remaining() > 0) {

                // Сохранить непереданную часть для следущего цикла передачи
                buffer.compact();

                // Выставляем бит OP_WRITE в 1
                // (подписываемся на флаг готовности сокета отправлять данные)
                setInterest(key, SelectionKey.OP_WRITE);
                // В следущем цикле будем отправлять
                // update selector -
                selector.wakeup();

            }
            // -------------------------------------------------------------------------
            // Все успешно записалось
            // data.remaining() == 0
            else {

                // refresh client TTL
                connectionList.update(id);

                connectionList.get(id).setData(null);

                // prepare buffer to next transmit
                buffer.clear();

                // Если подписывались на флаг готовности на отправку и успешно отправили
                // Выставляем бит OP_WRITE в 0 (отписываемся)
                removeInterest(key, SelectionKey.OP_WRITE);
            }
            // -------------------------------------------------------------------------

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Подгатавливает данные для записи в сокет
     * Подписывается на флаг возможности записи в сокет
     */
    private void writeChannel(SelectionKey key, ByteBuffer data) {

        // Т.к. все асинхронное (несколько потоков)
        // То одному и тому же клиенту могут начать отправлять одновременно нескольуо сообщений -
        // Надо делать очередь сообщений (на отправку) для клиента.
        // (Если не охота потом принимать байты сообщений в перемешанном порядке)
        // (Люди говорят для TCP такое можно устроить, для UDP - нет)
        // (Потом, например, удалять только те сообщения, котороые удалось доставить,
        // получится к-то сетевой мессенджер, да клиент еще в оффлайне может быть, очередь хранить надо)

        int id = (int)key.attachment();

        // Проверить нет ли текущих данных на отправку (в connectionList)
        // и если есть, то не отправлять - просто потерять это данные (ибо нефиг)

        // Можно, конечно валить все в сокет, (и больше ~3 метров в неблокирующем режиме не залезет)
        // дальше данные начнут теряться уже в сетевой подсистеме ядра


        if (connectionList.get(id).getData() != null) {
            System.out.println("Внимание - обнаружена попытка одновременной передачи, данные НЕ отправлены");
            return;
        }

        data.rewind();
        connectionList.get(id).setData(data);
        setInterest(key, SelectionKey.OP_WRITE);
    }


    private void setInterest(SelectionKey key, int interest) {

        if ((key.interestOps() & interest) == 0) {
            int current = key.interestOps();
            key.interestOps(current | interest);
        }
    }


    private void removeInterest(SelectionKey key, int interest) {

        if ((key.interestOps() & interest) != 0) {
            int current = key.interestOps();
            key.interestOps(current & ~interest);
        }
    }




    // ================================================================


    public static void main(String[] args) throws IOException {

        Thread t = new Thread(new FubarServer());
        t.setDaemon(false);
        t.start();
//
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ignore) {}
    }









    // =================================================================


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






}