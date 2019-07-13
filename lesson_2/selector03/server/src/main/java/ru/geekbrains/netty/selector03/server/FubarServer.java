package ru.geekbrains.netty.selector03.server;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import ru.geekbrains.netty.selector03.server.entities.Connection;
import ru.geekbrains.netty.selector03.server.entities.ConnectionList;
import ru.geekbrains.netty.selector03.server.entities.jobpool.BlockingJobPool;
import ru.geekbrains.netty.selector03.server.serverActions.DirectoryReader;
import static ru.geekbrains.netty.selector03.server.utils.Utils.isNullOrEmpty;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
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

                                readSocket(finalKey);
                                processInput(finalKey);

                                // Возвращаем подписку на флаг чтения новых данных из сокета
                                setInterest(finalKey, SelectionKey.OP_READ);
                                // Будим селектор (могли подойти новые данные)
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

            SeekableByteChannel data = new SeekableInMemoryByteChannel(welcomeString.getBytes());

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
    private void readSocket(SelectionKey key)  {

        int id = -1;

        try {

            SocketChannel client = (SocketChannel)key.channel();
            id = (int)key.attachment();
            Connection connection = connectionList.get(id);

            SeekableByteChannel data = connection.getReceiveChannel();
            ByteBuffer buffer = connection.getReadBuffer();

            // подготавливаем буфер для чтения
            buffer.clear();

            boolean someDataHasReadied = false;

            int read;

            // read >  0  - readied some data
            // read =  0  - no data available
            // read = -1  - connection closed / EOF

            while ((read = client.read(buffer)) > 0) {
                
                buffer.flip();

                // устанавливаем флаг что что-то смогли прочитать из сокета
                someDataHasReadied = true;

                // Parse header if didn't do it before
                // Узнаем тип сообщения и его размер
                if (!connection.isHeaderHasReaded()) {
                    parseHeader(connection);
                }

                // уменьшаем количество оставшихся байт сообщения для чтения
                connection.setRemainingBytesToRead(connection.getRemainingBytesToRead() - read);


                data.write(buffer);
                buffer.clear();
            }

            // -------------------------------------------------
            // Если хоть что-то передалось
            if (someDataHasReadied) {
                // refresh client TTL
                connectionList.update(id);
            }
            // -------------------------------------------------

            // Remote endpoint close connection
            if (read < 0) {
                System.out.println(key.attachment() + " отключился");
                connectionList.remove(id); // will close socket channel
            }

            // Приняли все байты сообщения
            if(connection.getRemainingBytesToRead() == 0)  {

                // возвращаем обратно возможность разбирать заголовок нового сообщения
                // восстанавливаем новый цикл приема сообщений
                connection.setHeaderHasReaded(false);
            }

        }
        // Remote endpoint close connection
        // maybe not happened: handled in "if (read < 0)"
        catch(ClosedChannelException e) {
            System.out.println(key.attachment() + " отключился");
            connectionList.remove(id); // will close socket channel
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }




    private void parseHeader(Connection connection) {


        ByteBuffer buffer = connection.getReadBuffer();
        //int bufferLimit = buffer.limit();
        buffer.rewind();

        // get payload length
        connection.setRemainingBytesToRead(buffer.getLong());

        // увеличим количество байт для чтения на размер заголовка (8+1)
        // т.к. в цикле readSocket(..)
        // В количество прочитанных байт из сокета 'read' войдет как длина заголовока,
        // так и число прочтенных байт самого сообщения
        connection.setRemainingBytesToRead(connection.getRemainingBytesToRead() + 8 + 1);

        // get message type
        connection.setMessageType(Connection.MessageType.parse(buffer.get()));
//
//        // Устанавливаем подходящий канал
//        switch (connection.getMessageType()) {
//            case TEXT:
//
//                break;
//            case BINARY:
//
//                break;
//        }


        connection.setHeaderHasReaded(true);
    }



    private void writeSocket(SelectionKey key)  {

        int id = -1;

        try {
            
            System.out.println("writeSocket");

            SocketChannel client = (SocketChannel)key.channel();
            id = (int)key.attachment();
            Connection connection = connectionList.get(id);

            ByteBuffer buffer = connection.getWriteBuffer();
            SeekableByteChannel data = connection.getTransmitChannel();

            boolean someDataHasSend;

            int wrote;
            int dataReaded;

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written    // need register(selector, SelectionKey.OP_WRITE, id);

            // пишем в сокет, пока есть что передавать
            // и сокет принимает данные (не затопился)
            do {

                buffer.clear();

                // Add header if absent
                if (!connection.isHeaderHasWrited()) {
                    writeHeader(connection);
                }

                dataReaded = data.read(buffer);
                buffer.flip();
            }
            while ((wrote = client.write(buffer)) > 0 &
                   data.position() < data.size());

            someDataHasSend = wrote > 0;

            // -------------------------------------------------
            // Если хоть что-то передалось - refresh client TTL
            if (someDataHasSend) {
                connectionList.update(id);
            }
            // -------------------------------------------------

            // причем, если не отправилось по сети, то в buffer будет лежать кусок
            // (скопированный из data), который так и не отправился
            // буффер нужно очистить, а transmitChannel отмотать назад на размер прочтенных байт из data
            //
            // Это все к тому, то нельзя начинать передавать новые данные, пока по сети не передалось
            // текущее сообщение


            // -------------------------------------------------------------------------
            // Remote endpoint close connection
//            if (wrote < 0) {
//                System.out.println(key.attachment() + " отключился");
//                connectionList.remove(id); // will close socket channel
//            }
            // -------------------------------------------------------------------------


            // Не смогли передать все данные
            // Флудим сокет данными - он не успевает принимать на удаленном конце
            // регистрируемся на флаг что удаленный сокет может принимать сообщения
            // чтобы возобновить передачу как сокет будет готов передавать
            else if (data.position() < data.size()) {

                // Сохранить непереданный кусок данных для следущего цикла передачи
                // отмотаем transmitChannel назад на размер буффера
                data.position(data.position() - dataReaded);

                // Выставляем бит OP_WRITE в 1 (подписываемся на флаг готовности сокета отправлять данные)
                setInterest(key, SelectionKey.OP_WRITE);

                // будим селектор (будем отправлять данные в следущем цикле)
                selector.wakeup();

            }
            // -------------------------------------------------------------------------
            // Все успешно записалось, data.position == data.size
            else {

                // возвращаем обратно возможность писать заголовок для нового сообщения
                // восстанавливаем новый цикл записи сообщений
                connection.setHeaderHasWrited(false);

                // Закрываем канал (откуда писали в сокет)
                data.close();
                
                // обнуляем ссылку на transmitChannel
                // (Защита от записи в сокет нового сообщения,
                // если он еще не закончил передачу текущих данных)
                connection.setTransmitChannel(null);

                // очищаем буффер для следущей передачи
                buffer.clear();

                // отписываемся от оповещения что сокет готов передавать данные
                // Выставляем в ключе бит OP_WRITE в 0 (отписываемся)
                removeInterest(key, SelectionKey.OP_WRITE);
            }
            // -------------------------------------------------------------------------

        }
        // Remote endpoint close connection
        catch(ClosedChannelException e) {
            System.out.println(key.attachment() + " отключился");
            connectionList.remove(id); // will close socket channel
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Write header to connection.writeBuffer
     */
    private void writeHeader(Connection connection) {

        try {

            ByteBuffer buffer = connection.getWriteBuffer();
            assert buffer.position() == 0;

            // write message size
            buffer.putLong(connection.getTransmitChannel().size());

            // write message type
            buffer.put((byte)connection.getMessageType().getValue());

            connection.setHeaderHasWrited(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    // -------------------------------------------------------------------------------


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
    private void scheduleWrite(SelectionKey key, SeekableByteChannel data) {

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

        if (connection.getTransmitChannel() != null) {
            System.out.println("Внимание - обнаружена попытка одновременной передачи, данные НЕ отправлены");
            return;
        }

        // Задаем сокету данные на передачу
        connection.setTransmitChannel(data);

        setInterest(key, SelectionKey.OP_WRITE);
    }


    /**
     * Whether receive text command or binary data
     */
    private void processInput(SelectionKey key) {

        int id = (int)key.attachment();
        Connection connection = connectionList.get(id);

        if (connection.getMessageType() == Connection.MessageType.BINARY) {

         }



        processCommand(key, "123");
    }


    /**
     * Parse and process user commands
     * Then reply to user in TEXT MODE
     * (hope text reply will not concat with subsequent binary stream)
     */
    private void processCommand(SelectionKey key, String command) {

        String result = null;
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

                    // if file successfully opened to read
                    // store file channel
                    connection.setTransmitChannel(file.getChannel());

                    //result = Connection.MessageType.BINARY.toString();
                }
                catch (Exception e) {
                    result = "I/O error";
                    e.printStackTrace();
                }

                break;// ---------------------------------------------------------


            case "set":

                // file name not specified
                if (parts.length < 2 ||
                    isNullOrEmpty(parts[1])) {

                    result = "invalid command args";
                    break;
                }

                // set file
                try {
                    filePath = Paths.get(dataRoot + parts[1]);

                    RandomAccessFile file = new RandomAccessFile(filePath.toString(), "w");

                    // if file successfully opened to write
                    // store file channel
                    connection.setReceiveChannel(file.getChannel());
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


        // Заланируем отправку клиенту результата выполнения команды
        if (!isNullOrEmpty(result)) {
            result +="\n";

            SeekableByteChannel data = new SeekableInMemoryByteChannel(result.getBytes());

            // schedule sending greeting
            scheduleWrite(key, data);
        }
    }




    /*
    private SeekableByteChannel wrapStringToChannel(String message)  {

        SeekableByteChannel result = null;

        try {

            PipedInputStream pipedInputStream = new PipedInputStream();
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            pipedInputStream.connect(pipedOutputStream);

            pipedOutputStream.write(message.getBytes());
            result = (SeekableByteChannel)Channels.newChannel(pipedInputStream);

        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
    */


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



    public void onDone(Void v) {
        System.out.println("Done");
    }
}



/*

result = new String(bufferStream.toByteArray(), StandardCharsets.UTF_8).trim();



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