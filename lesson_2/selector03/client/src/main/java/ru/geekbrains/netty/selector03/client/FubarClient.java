package ru.geekbrains.netty.selector03.client;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import ru.geekbrains.netty.selector03.common.entities.Connection;
import ru.geekbrains.netty.selector03.common.entities.MessageType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static ru.geekbrains.netty.selector03.common.entities.Utils.channelToString;

public class FubarClient implements Runnable {

    private SocketChannel socketChannel;
    private static final int PORT_NUMBER = 8000;
    private String dataRoot;
    private Connection connection;

    public FubarClient() throws IOException {

        // Будут проблемы с путями
        dataRoot = System.getProperty("user.dir") + "/client/data/";  //(? File.separator)
        Files.createDirectories(Paths.get(dataRoot));

        // in blocking mode
        socketChannel = SocketChannel.open();
        socketChannel.connect((new InetSocketAddress("127.0.0.1", PORT_NUMBER)));

        //noinspection ConstantConditions
        connection = new Connection(socketChannel);
    }


    @Override
    public void run() {

        try {

            // reading server greeting
            readSocket();

            // print greeting to user
            processResponse();

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            // endless loop
            while(true) {

                // get user input
                String input = br.readLine();

                parseUserInput(input);

                //send to server
                SeekableByteChannel data = new SeekableInMemoryByteChannel(input.getBytes());
                connection.setTransmitChannel(data);
                writeSocket();

                // reading response
                readSocket();

                processResponse();


            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     * Читаем из сокета данные, сколько их там накопилось
     * Учитывая длину сообщения из заголовка
     */
    private void readSocket()  {

        try {

            //System.out.println("readSocket");

            SocketChannel client = connection.getChannel();
            ByteBuffer buffer = connection.getReadBuffer();

            // Изначально не знаем что приедет - текст или файл
            SeekableByteChannel data = connection.getReceiveChannel();

            int read;

            // read >  0  - readied some data
            // read =  0  - no data available (end of stream)
            // read = -1  - closed connection



            // подготавливаем буфер для чтения
            buffer.clear();

            // см FubarServer
            if(!connection.isReceiveHeaderPresent()) {
                buffer.limit(8 + 1);
            }
            else {

                buffer.limit((int)Math.min(
                        (long)buffer.capacity(),
                        connection.remainingBytesToRead()));
            }

            while ((read = client.read(buffer)) > 0) {

                buffer.flip();

//                byte[] bytes = new byte[buffer.limit()];
//                buffer.get(bytes);
//                System.out.println("R: " + new String(bytes, StandardCharsets.UTF_8));
//                buffer.rewind();


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
                        data = connection.createReceiveFile();
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

                // преодалеваем упирание в блокирующий сокет (на чтение)
                if (connection.remainingBytesToRead() == 0) {
                    break;
                }
            }

            // возвращаем обратно возможность разбирать заголовок нового сообщения
            connection.setReceiveHeaderPresent(false);

            // Remote endpoint close connection
            if (read < 0) {
                System.out.println("потеряна связь с сервером");
                connection.close();
                return;
            }


        }
        // Remote endpoint close connection
        catch (Exception e) {
            if (e instanceof ClosedChannelException) {
                System.out.println("потеряна связь с сервером");
                // Remote endpoint close connection
                // (maybe not handled in "if (read < 0)")
                connection.close(); // will close socket channel
            }
            e.printStackTrace();
        }

        //System.out.println("readSocket end");
    }





    private void writeSocket()  {

        try {

            //System.out.println("writeSocket");

            SocketChannel client = connection.getChannel();
            ByteBuffer buffer = connection.getWriteBuffer();
            SeekableByteChannel data = connection.getTransmitChannel();

            //int wrote;

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written // socket stall

            do {
                buffer.clear();

                // Add header if absent
                if (!connection.isTransmitHeaderPresent()) {
                    connection.writeHeader();
                }

                data.read(buffer);
                buffer.flip();

//                byte[] bytes = new byte[buffer.limit()];
//                buffer.get(bytes);
//                System.out.println("T: " + new String(bytes, StandardCharsets.UTF_8));
//                buffer.rewind();

                // пишем до упора, флудим сокет, висим на client.write(...)
                // пока все не пролезет или не упадем
                client.write(buffer);
            }
            while (data.position() < data.size());

            // -------------------------------------------------------------------------
            // Все успешно записалось, data.position == data.size

            // возвращаем обратно возможность писать заголовок для нового сообщения
            // восстанавливаем новый цикл записи сообщений
            connection.setTransmitHeaderPresent(false);

            // Закрываем файловый канал (откуда писали в сокет)
            if (connection.getChannelType(data) == MessageType.BINARY) {
                data.close();
            }
            // Если это был текстовый канал, то ничего не делаем,
            // он там переиспользуется

            // очищаем буффер для следущей передачи
            buffer.clear();

            // -------------------------------------------------------------------------

        }
        // Remote endpoint close connection
        catch (Exception e) {
            if (e instanceof ClosedChannelException) {
                System.out.println("потеряна связь с сервером");
                // Remote endpoint close connection
                // (maybe not handled in "if (read < 0)")
                connection.close(); // will close socket channel
            }
            e.printStackTrace();
        }
    }



    private void processResponse() {


        try {

            SeekableByteChannel receiveChannel =  connection.getReceiveChannel();

            // Text message
            if (connection.getChannelType(receiveChannel) == MessageType.TEXT) {

                // display to user
                String response = channelToString(receiveChannel);
                System.out.println(response);

                // будем использовать повторно, без создания нового channel
                // receiveChannel буфферезируется (backed by) bufferedReceiveChannel
                // поэтому не закрываем, а truncate до 0
                receiveChannel.position(0);
                receiveChannel.truncate(0);
            }
            else {

                // working with files


                // там в прошлом через команду уже был настроен файл для приема
                // И в readSocket() файл уже записался.
                // Поэтому просто закрываем
                receiveChannel.close();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // -----------------------------------------------------------------------------


    private void parseUserInput(String command) {

        String[] parts = command.split(" ");

        switch (parts[0]) {

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
                    RandomAccessFile file = new RandomAccessFile(filePath.toString(), "r");

                    // if file successfully opened to read
                    // store file channel
                    //connection.setTransmitChannel(file.getChannel());

                    // А тут будем отвечать клиенту файлом
                    data = file.getChannel();
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

    }


    // ==============================================================




    public static void main(String[] args) throws IOException {

        Thread t = new Thread(new FubarClient());
        //t.setDaemon(false);
        t.start();
    }



}
