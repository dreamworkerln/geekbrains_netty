package ru.geekbrains.netty.selector03.client;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import ru.geekbrains.netty.selector03.common.entities.Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

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
            SeekableByteChannel data = connection.getReceiveChannel();

            // подготавливаем буфер для чтения
            buffer.clear();

            int read;

            // read >  0  - readied some data
            // read =  0  - no data available (end of stream)
            // read = -1  - closed connection

            while ((read = client.read(buffer)) > 0) {

                buffer.flip();

                // Parse header if didn't do it before
                // Узнаем тип сообщения и его размер
                if (!connection.isHeaderHasReaded()) {
                    connection.parseHeader();

                    // Определяемся, куда сохранять данные
                    // Если это текстовый ответ на мой запрос, выделим под него канал
                    if(connection.getMessageType() == Connection.MessageType.TEXT) {

                        // после обработки ответа этот канал будет закрыт
                        connection.setReceiveChannel(new SeekableInMemoryByteChannel());
                    }
                    // Если же это файл, то нас уже заранее заведен под него канал
                    // и receiveChannel уже присвоен к RandomAccessFile
                }

                // уменьшаем количество оставшихся байт сообщения для чтения
                connection.setRemainingBytesToRead(connection.getRemainingBytesToRead() - read);

                data.write(buffer);
                buffer.clear();

                // обходим упор в блокирующий сокет
                if (connection.getRemainingBytesToRead() <= 0) {
                    break;
                }
            }

            // возвращаем обратно возможность разбирать заголовок нового сообщения
            connection.setHeaderHasReaded(false);

            // Remote endpoint close connection
            if (read < 0) {
                System.out.println("потеряна связь с сервером");
                connection.close();
            }


        }
        // Remote endpoint close connection
        catch(ClosedChannelException e) {
            System.out.println("потеряна связь с сервером");
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            connection.close();
        }

        //System.out.println("readSocket end");
    }





    private void writeSocket()  {

        try {

            System.out.println("writeSocket");

            SocketChannel client = connection.getChannel();
            ByteBuffer buffer = connection.getWriteBuffer();
            SeekableByteChannel data = connection.getTransmitChannel();

            //int wrote;

            // wrote  >  0  - wrote some data
            // wrote  =  0  - no data written // socket stall

            do {
                buffer.clear();

                // Add header if absent
                if (!connection.isHeaderHasWrited()) {
                    connection.writeHeader();
                }

                data.read(buffer);
                buffer.flip();

                // пишем до упора, флудим сокет, висим на client.write(...)
                // пока все не пролезет или не упадем
                client.write(buffer);
            }
            while (data.position() < data.size());

            // -------------------------------------------------------------------------
            // Все успешно записалось, data.position == data.size

            // возвращаем обратно возможность писать заголовок для нового сообщения
            // восстанавливаем новый цикл записи сообщений
            connection.setHeaderHasWrited(false);

            // Закрываем канал (откуда писали в сокет)
            data.close();

            // очищаем буффер для следущей передачи
            buffer.clear();

            // -------------------------------------------------------------------------

        }
        // Remote endpoint close connection
        catch(ClosedChannelException e) {
            System.out.println("потеряна связь с сервером");
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            connection.close(); // hm.. howto join this with previous one ?
        }
    }



    private void processResponse() {


        try {
            if (connection.getMessageType() == Connection.MessageType.TEXT) {

                // display to user
                String response = channelToString(connection.getReceiveChannel());
                System.out.println(response);
            }
            else {

                // working with files
            }

            // Закрываем канал с принятыми и обработанными от сервера данными
            connection.getReceiveChannel().close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // ==============================================================




    public static void main(String[] args) throws IOException {

        Thread t = new Thread(new FubarClient());
        //t.setDaemon(false);
        t.start();
    }



}
