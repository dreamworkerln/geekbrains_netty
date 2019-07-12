package ru.geekbrains.netty.selector03.server;

import ru.geekbrains.netty.selector03.server.entities.Connection;
import ru.geekbrains.netty.selector03.server.entities.ConnectionList;
import ru.geekbrains.netty.selector03.server.entities.jobpool.BlockingJobPool;
import ru.geekbrains.netty.selector03.server.serverActions.DirectoryReader;
import static ru.geekbrains.netty.selector03.server.utils.Utils.isNullOrEmpty;
import static ru.geekbrains.netty.selector03.server.utils.Utils.copyBuffer;

import java.io.*;
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
                        acceptSocket(key);
                    }

                    if (key.isReadable()) {

                        // Снипмаем подписку о получении новых данных в сокете
                        // Чтобы не бегать бесконечно в цикле select
                        // Пока поток из пула читает данные из сокета
                        removeInterest(key, SelectionKey.OP_READ);

                        // Читаем данные в отдельном потоке
                        // Затем назначаем ее на выполнение
                        // (выполняться будет потом в другом потоке)
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {

                            try {

                                String command = readSocket(finalKey);
                                processCommand(finalKey, command);

                                // Возвращаем подписку на флаг чтения новых данных из сокета
                                setInterest(finalKey, SelectionKey.OP_READ);
                                // Будим селектор
                                selector.wakeup();
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                            return null;
                        });
                    }



                    // Интерес на запись выставляется отдельно
                    // вручную при желании что-либо передать
                    // либо внутри writeSocket(...) если затопился сокет и отправка не удалась
                    if (key.isWritable()) {

                        // Чтобы не бегать бесконечно в цикле select
                        // Пока потоки из пула пишут в сокеты
                        removeInterest(key, SelectionKey.OP_WRITE);

                        // Пишем в отдельном потоке
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {

                            try {
                                writeSocket(finalKey);
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                            return null;
                        });

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void acceptSocket(SelectionKey key) {


        try {

            System.out.println("acceptSocket");

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

            // регистрируемся на OP_READ на сокете клиента
            SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ, id);

            connectionList.add(clientKey);


            //ByteBuffer welcomeBuf = ByteBuffer.wrap(welcomeString.getBytes());


            PipedInputStream pipedInputStream = new PipedInputStream();
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            pipedInputStream.connect(pipedOutputStream);

            pipedOutputStream.write(welcomeString.getBytes());
            //ByteArrayOutputStream outStream = new ByteArrayOutputStream(welcomeString.length());
            //outStream.write(welcomeString.getBytes());

            ReadableByteChannel data = Channels.newChannel(pipedInputStream);

            // schedule sending greeting
            scheduleWrite(clientKey, data);

            System.out.println("Подключился новый клиент #" + id);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    /**
     * Читаем из сокета данные, сколько их там накопилось
     * Учитывая длину сообщения из заголовка
     */
    private String readSocket(SelectionKey key)  {

        String result = null;

        try {

            SocketChannel client = (SocketChannel)key.channel();
            int id = (int)key.attachment();
            Connection connection = connectionList.get(id);

            WritableByteChannel data = connection.getWriteChannel();
            ByteBuffer buffer = connection.getReadBuffer();

            // подготавливаем буфер для чтения
            buffer.reset();

            int read;

            // read >  0  - readied some data
            // read =  0  - no data available
            // read = -1  - connection closed

            while ((read = client.read(buffer)) > 0) {

                // Parse header
                if (!connection.isHeaderReceived()) {
                    parseHeader();
                }

                data.write(buffer);
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

    private void parseHeader() {

        

    }



    private void writeSocket(SelectionKey key)  {

        try {

            System.out.println("writeSocket");

            SocketChannel client = (SocketChannel)key.channel();
            int id = (int)key.attachment();
            Connection connection = connectionList.get(id);

            ByteBuffer buffer = connection.getWriteBuffer();
            ReadableByteChannel data = connection.getReadChannel();

            boolean someDataHasSend = false;

            int wrote = 0;

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written // need register(selector, SelectionKey.OP_WRITE, id);
            // wrote  = -1  - connection closed


            // пишем в сокет, пока есть что передавать
            // и сокет принимает данные (не затопился)
            do {

                someDataHasSend = (!someDataHasSend) && (wrote > 0);

                // не трогаем буфер
                // он может содержать данные, которые он не смог отправить в прошлый раз
                data.read(buffer);
                buffer.flip();
            }
            while ((wrote = client.write(buffer)) > 0 &
                   buffer.remaining() > 0);
            // -------------------------------------------------
            // Если хоть что-то передалось
            if (someDataHasSend) {
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
            // Не смоглипередать все данные
            // Флудим сокет данными - он не успевает принимать на удаленном конце
            // регистрируемся на флаг что удаленный сокет может принимать сообщения
            else if (buffer.remaining() > 0) {

                // Сохранить непереданный кусок данных для следущего цикла передачи
                buffer.compact();

                // Выставляем бит OP_WRITE в 1 (подписываемся на флаг готовности сокета отправлять данные)
                setInterest(key, SelectionKey.OP_WRITE);
                // update selector (будем отправлять в следущем цикле)
                selector.wakeup();

            }
            // -------------------------------------------------------------------------
            // Все успешно записалось
            // data.remaining() == 0
            else {

                // refresh client TTL
                connectionList.update(id);

                // Помечаем, что нет данных на передачу
                connection.setReadChannel(null);

                // prepare buffer to next transmit
                buffer.clear();

                // отписываемся от оповещения что сокет может отправлять данные
                // Выставляем в ключе бит OP_WRITE в 0 (отписываемся)
                removeInterest(key, SelectionKey.OP_WRITE);
            }
            // -------------------------------------------------------------------------

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void setInterest(SelectionKey key, int interest) {

        if (key.isValid() &&
            (key.interestOps() & interest) == 0) {

            int current = key.interestOps();
            key.interestOps(current | interest);
        }
    }


    private void removeInterest(SelectionKey key, int interest) {

        if (key.isValid() &&
            (key.interestOps() & interest) != 0) {

            int current = key.interestOps();
            key.interestOps(current & ~interest);
        }
    }




    // =============================================================================




    /**
     * Планрует запись в сокет
     * <br>
     * Подготавливает данные для записи в сокет,
     * подписывается(устанавливает) на флаг возможности записи в сокет
     */
    private void scheduleWrite(SelectionKey key, ReadableByteChannel data) {

        // Т.к. все асинхронное (несколько потоков)
        // То одному и тому же клиенту могут начать отправлять одновременно несколько сообщений -
        // Надо делать очередь сообщений (на отправку) для клиента.
        // (Если не охота потом принимать байты(куски байт) сообщений в перемешанном порядке)
        // (Люди говорят для TCP такое можно устроить, для UDP - нет)
        // (Потом, например, удалять только те сообщения, котороые удалось доставить, и т.д.)

        int id = (int)key.attachment();
        Connection connection = connectionList.get(id);

        // Проверить нет ли текущих данных на отправку (в connectionList)
        // и если есть, то не отправлять - просто потерять это данные (ибо нефиг)

        // Можно, конечно валить все в сокет, (и больше ~3 метров в неблокирующем режиме не залезет)
        // дальше данные начнут теряться уже в сетевой подсистеме ядра при переполнении буффера сокета

        if (connection.getReadChannel() != null) {
            System.out.println("Внимание - обнаружена попытка одновременной передачи, данные НЕ отправлены");
            return;
        }

        connection.setReadChannel(data);
        setInterest(key, SelectionKey.OP_WRITE);
    }



    /**
     * Parse and process user commands
     * Then reply to user in TEXT MODE
     * (hope text reply will not concat with subsequent binary stream)
     */
    private void processCommand(SelectionKey key, String command) {

        String result;
        int id = (int)key.attachment();
        Connection connection = connectionList.get(id);

        // No input
        if (isNullOrEmpty(command)) {
            return;
        }

        String[] parts = command.split(" ");

        switch (parts[0]) {

            case "list":
                Function<String,String> dirNfo = new DirectoryReader();
                result = dirNfo.apply(dataRoot);

                break;// ---------------------------------------------------------


            case "get":

                // file name not specified
                if (parts.length < 2 ||
                    isNullOrEmpty(parts[1])) {

                    result = "invalid command args";
                    break;
                }


                Path filePath = Paths.get(dataRoot + parts[1]);

                // file not exists
                if (!Files.exists(filePath)) {
                    result = "file not exists";
                    break;
                }

                // get file
                try {
                    RandomAccessFile file = new RandomAccessFile(filePath.toString(), "r");

                    // if file successfully opened
                    connection.setFile(file);

                    // Schedule switching to BINARY mode on next select loop.
                    // Switching performed in write handlers, after sending
                    // to client text notification that told to client
                    // to switch to BINARY mode as server did
                    connection.setNextMode(Connection.Mode.BINARY);

                    result = Connection.Mode.BINARY.toString();
                }
                catch (Exception e) {
                    result = "I/O error";
                    e.printStackTrace();
                }

                break;// ---------------------------------------------------------



            default:
                result = "unknown command";
                 break;// ---------------------------------------------------------
        }


        // Отправляем (планируем отправку) клиенту текст (результат выполнения команды)
        if (!isNullOrEmpty(result)) {
            result +="\n";
            ByteBuffer writeBuffer = ByteBuffer.wrap(result.getBytes());
            writeChannelText(key, writeBuffer);
        }
    }


    // =============================================================================


    public static void main(String[] args) throws IOException {

        Thread t = new Thread(new FubarServer());
        t.setDaemon(false);
        t.start();
    }









    // =============================================================================


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



/*



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



 */