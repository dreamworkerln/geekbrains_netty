package ru.geekbrains.netty.selector03.server;

import ru.geekbrains.netty.selector03.common.entities.Connection;
import ru.geekbrains.netty.selector03.common.entities.MessageType;
import ru.geekbrains.netty.selector03.server.entities.ConnectionList;
import ru.geekbrains.netty.selector03.server.entities.jobpool.BlockingJobPool;
import ru.geekbrains.netty.selector03.server.serverActions.DirectoryReader;

import static ru.geekbrains.netty.selector03.common.entities.Utils.StringTochannel;
import static ru.geekbrains.netty.selector03.common.entities.Utils.channelToString;
import static ru.geekbrains.netty.selector03.common.entities.Utils.isNullOrEmpty;

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
    private final String welcomeString = "Fubar Transfer Protocol server приветствует вас.";

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
        dataRoot = System.getProperty("user.dir") + "/data/";  //(? File.separator)
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

                // ...
                if (selector.selectedKeys().size() == 0) {
                    continue;
                }

                //System.out.println("SELECTING: " + selector.selectedKeys().size());

                while (it.hasNext()) {

                    key = it.next();

                    //System.out.println("KEY INTERESTS: " + key.interestOps());
                    //System.out.println("KEY READY    : " + key.readyOps());


                    it.remove();

                    // skip invalid keys (disconnected channels)
                    if (!key.isValid())
                        continue;

                    if (key.isAcceptable()) {
                        acceptSocket(key);
                    }

                    if (key.isReadable()) {

                        // Снипмаем подписку о получении новых данных в сокете
                        // пока поток из пула читает данные из сокета
                        // Чтобы не бегать бесконечно в цикле select
                        removeInterest(key, SelectionKey.OP_READ);

                        // Читаем данные в отдельном потоке
                        // Если прочиталось все сообщение, то назначаем его на выполнение
                        // (выполняться будет потом в другом потоке)
                        // Если целиком не прочиталось - будет дочитываться в других циклах select
                        SelectionKey finalKey = key;
                        jobPool.add(() -> {

                            try {

                                System.out.println("OP_READ job start");

                                // Если сообщение принято целиком,
                                // то обработать его
                                if (readSocket(finalKey)) {
                                    processInput(finalKey);
                                }

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

                                //System.out.println("OP_WRITE job start");

                                writeSocket(finalKey);


                                // Возвращаем подписку на флаг чтения новых данных из сокета
                                //setInterest(finalKey, SelectionKey.OP_WRITE);
                                // Будим селектор (могли подойти новые данные)
                                //selector.wakeup();


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
            setInterest(clientKey, SelectionKey.OP_WRITE);

            connectionList.add(clientKey);

            //SeekableByteChannel data = new SeekableInMemoryByteChannel(welcomeString.getBytes());

            Connection connection = connectionList.get(id);
            SeekableByteChannel data = connection.getBufferedTransmitChannel();
            // will write greeting to data
            StringTochannel(welcomeString, data);

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
     *
     * @return boolean сообщение принято целиком
     */
    private boolean readSocket(SelectionKey key)  {

        boolean result = false;
        int id = -1;

        try {

            System.out.println("readSocket");

            SocketChannel client = (SocketChannel)key.channel();
            id = (int)key.attachment();
            Connection connection = connectionList.get(id);

            // Изначально не знаем что приедет - текст или файл
            SeekableByteChannel data = connection.getReceiveChannel();

            ByteBuffer buffer = connection.getReadBuffer();

            boolean someDataHasReadied = false;

            int read; // сколько байт прочли из сокета

            // read >  0  - readied some data
            // read =  0  - no data available (end of stream)
            // read = -1  - closed connection


            // подготавливаем буфер для чтения
            buffer.clear();

            // Еще не читали заголовок сообщения из сокета
            // Устанавливаем limit буффера в размер заголовка
            // и будем читать только заголовок
            if(!connection.isReceiveHeaderPresent()) {
                buffer.limit(8 + 1); // length + type
            }
            else {

                // => проблема с ненужным вычитыванием начальных байтов следущего сообщения
                // если сообщения идут подряд -
                // то надо в начале вычитать только хедер, узнать сколько байт надо принять
                // а потом выставить limit буфера в
                // min(buffer.capacity, bytesRemaining)
                // Чтобы буфер не прочел данные за концом текущего сообщения
                // это будет начальные байты(заголовк) следущего сообщения
                buffer.limit((int)Math.min(
                        (long)buffer.capacity(),
                        connection.remainingBytesToRead()));
            }

            while ((read = client.read(buffer)) > 0) { // ----------------------------------------------

                buffer.flip();

                // устанавливаем флаг что что-то смогли прочитать из сокета
                someDataHasReadied = true;

                // Parse header if didn't do it before ---------------------------------------
                // Узнаем тип сообщения и его размер
                if (!connection.isReceiveHeaderPresent()) {
                    MessageType messageType = connection.parseHeader();

                    // Определяемся, куда сохранять данные
                    if(messageType == MessageType.TEXT) {

                        // берем из буферный канал для текста
                        data = connection.getBufferedReceiveChannel();
                    }
                    else {
                        // пишем в файл
                        data = connection.createFileChannel(connection.getReceiveFilePath(), "w");
                    }
                    // устанавливаем выбранный канал для connection в качестве канала-приемника
                    connection.setReceiveChannel(data);

                } // -------------------------------------------------------------------------

                // уменьшаем количество оставшихся байт сообщения для чтения
                connection.decreaseRemainingBytesToRead(read);


                assert data != null;
                // пишем из буфера в канал
                data.write(buffer);

                // опять настраиваем буфер, чтоб жизнь медом не казалась
                buffer.rewind();
                buffer.limit((int)Math.min(
                        (long)buffer.capacity(),
                        connection.remainingBytesToRead()));
            } // ------------------------------------------------------------------------------------

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
                return false;
            }

            // Тут еще возможен вариант:
            // прочли не все, возможно оставшиеся байты сообщения
            // прижут позднее

            // Приняли все байты сообщения
            if(connection.remainingBytesToRead() == 0)  {

                // Сообщение прочиталось целиком
                result = true;

                // возвращаем обратно возможность разбирать заголовок нового сообщения
                connection.setReceiveHeaderPresent(false);

                // очищаем буффер для следущего приема (не обязательно)
                buffer.clear();

            }

//            assert data != null;
//            long pos = data.position();
//            System.out.println("R: " + channelToString(data));
//            data.position(pos);


        }
        catch (Exception e) {
            if (e instanceof ClosedChannelException) {
                System.out.println(key.attachment() + " отключился");
                // Remote endpoint close connection
                // (maybe not handled in "if (read < 0)")
                connectionList.remove(id); // will close socket channel
            }
            e.printStackTrace();
        }



        System.out.println("readSocket END");

        return result;
    }





    private void writeSocket(SelectionKey key)  {

        int id = -1;

        try {

            //System.out.println("writeSocket");

            SocketChannel client = (SocketChannel)key.channel();
            id = (int)key.attachment();
            Connection connection = connectionList.get(id);

            ByteBuffer buffer = connection.getWriteBuffer();
            SeekableByteChannel data = connection.getTransmitChannel();

            boolean someDataHasSend = false;

            int wrote;    // сколько байт записали в сокет
            int dataRead; // сколько байт прочли из канала

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written    // need register(selector, SelectionKey.OP_WRITE, id);

            // пишем в сокет, пока есть что передавать
            // и сокет принимает данные (не затопился)
            while (data.position() < data.size()) {

                // Add header if absent
                if (!connection.isTransmitHeaderPresent()) {
                    connection.writeHeader();
                }

                //  читаем из канала в буффер
                dataRead = data.read(buffer);
                buffer.flip();

                int remaining = buffer.remaining();

                // пишем в сокет
                wrote = client.write(buffer);

                if (!someDataHasSend) {
                    someDataHasSend = wrote > 0;
                }

                // что не залезло в сокет помещаем в начало буфера
                buffer.compact();

                // socket stall
                // оставляем сокет в покое
                if (remaining != wrote) {
                    //System.out.println("WR: " + wrote);
                    break;
                }





//                if (wrote == 0) {
//                    break;
//                }
            }

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


            // Не смогли передать все данные
            // Флудим сокет данными - он не успевает принимать на удаленном конце
            // регистрируемся на флаг что удаленный сокет может принимать сообщения
            // чтобы возобновить передачу как сокет будет готов передавать
            if (data.position() < data.size()) {

                // Сохранить непереданный кусок данных для следущего цикла передачи
                // отмотаем transmitChannel назад на размер данных в буффере (которые не передались)

                //System.out.println(data.position() + " / " + data.size());

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
                connection.setTransmitHeaderPresent(false);

                //System.out.println(data.position() + " / " + data.size());

                // Закрываем файловый канал (откуда писали в сокет)
                if (connection.getChannelType(data) == MessageType.BINARY) {
                    data.close();
                }
                // Если это был текстовый канал, то ничего не делаем,
                // он там переиспользуется


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


//            long pos = data.position();
//            System.out.println("T: " + channelToString(data));
//            data.position(pos);

        }
        catch (Exception e) {
            if (e instanceof ClosedChannelException) {
                System.out.println(key.attachment() + " отключился");
                // Remote endpoint close connection
                connectionList.remove(id); // will close socket channel
            }
            e.printStackTrace();
        }

        //System.out.println("writeSocket END");
    }

    // -------------------------------------------------------------------------------


    private void setInterest(SelectionKey key, int interest) {

        //System.out.println("setInterest ON: " + interest);

        if (key.isValid() &&
            (key.interestOps() & interest) == 0) {

            int current = key.interestOps();
            key.interestOps(current | interest);
        }
    }


    private void removeInterest(SelectionKey key, int interest) {

        //System.out.println("setInterest OFF: " + interest);

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

        try {
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
            data.position(0);
            connection.setTransmitChannel(data);

            setInterest(key, SelectionKey.OP_WRITE);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }






    /**
     * Process received channel (containing text command or file)
     */
    private void processInput(SelectionKey key) {

        try {
            int id = (int)key.attachment();
            Connection connection = connectionList.get(id);
            SeekableByteChannel receiveChannel =  connection.getReceiveChannel();

            // Text message
            if (connection.getChannelType(connection.getReceiveChannel()) == MessageType.TEXT) {

                String command = channelToString(receiveChannel);

                processCommand(key, command);

                // будем использовать повторно, без создания нового channel
                // receiveChannel backed by bufferedReceiveChannel
                // поэтому не закрываем, а truncate до 0
                receiveChannel.position(0);
                receiveChannel.truncate(0);
            }
            // File message
            else {

                // там в прошлом через команду уже был настроен файл для приема
                // И в readSocket() файл уже записался.
                // Поэтому просто закрываем
                receiveChannel.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * Parse and process user commands
     * Then reply to user in TEXT MODE
     * (hope text reply will not concat with subsequent binary stream)
     */
    private void processCommand(SelectionKey key, String command) {

        SeekableByteChannel data = null;
        String textResponse = null;

        int id = (int)key.attachment();
        Connection connection = connectionList.get(id);

//        // No input
//        if (isNullOrEmpty(command)) {
//            return;
//        }

        String[] parts = command.split(" ");

        switch (parts[0]) {

            case "ls":
                Function<String,String> dirNfo = new DirectoryReader();
                textResponse = dirNfo.apply(dataRoot);

                break;// ---------------------------------------------------------


            case "get":

                // file name not specified
                if (parts.length < 2 ||
                    isNullOrEmpty(parts[1])) {

                    textResponse = "invalid command args";
                    break;
                }


                Path filePath = Paths.get(dataRoot + parts[1]);

                // file not exists
                if (!Files.exists(filePath)) {
                    textResponse = "file not exists";
                    break;
                }

                // get file
                try {
                    // будем отвечать клиенту файловым каналом
                    data = connection.createFileChannel(filePath, "r");
                }
                catch (Exception e) {
                    textResponse = "I/O error";
                    e.printStackTrace();
                }

                break;// ---------------------------------------------------------


            case "set":

                // file name not specified
                if (parts.length < 2 ||
                    isNullOrEmpty(parts[1])) {

                    textResponse = "invalid command args";
                    break;
                }

                // set file
                try {
                    filePath = Paths.get(dataRoot + parts[1]);

                    connection.setReceiveFilePath(filePath);

                    textResponse = "ready to receive";
                }
                catch (Exception e) {
                    textResponse = "I/O error";
                    e.printStackTrace();
                }

                break;// ---------------------------------------------------------

            case "":

                textResponse = "nop";

                break;// ---------------------------------------------------------


            default:
                textResponse = "unknown command";
                break;// ---------------------------------------------------------
        }


        // Конвертируем текст - результат выполнения команды в channel
        if (!isNullOrEmpty(textResponse)) {

            data = connection.getBufferedTransmitChannel();
            // will write textResponse to data
            StringTochannel(textResponse, data);
        }

        assert data != null;

        // schedule sending response to command (text)
        scheduleWrite(key, data);
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
        //t.setDaemon(false);
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
        //System.out.println("Done");
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