package ru.geekbrains.netty.selector03.common.entities;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Path;
import java.time.Instant;

public class Connection {

    public static final int BUFFER_SIZE = 1024*1024; // read and write buffer size


    private SelectionKey key;
    private SocketChannel channel;  // сокет
    private ByteBuffer readBuffer;  // промежуточный буффер на чтение
    private ByteBuffer writeBuffer; // промежуточный буффер на запись
    private Instant time;
    private SeekableByteChannel transmitChannel;  // канал на передачу данных клиенту
    private SeekableByteChannel receiveChannel;   // канал на прием данных от клиента

    // чтобы не плодить каналов через new()
    public SeekableByteChannel bufferedTransmitChannel; // Используется для передачи текстовых данных
    private SeekableByteChannel bufferedReceiveChannel; // Используется для приема текстовых данных

    private boolean transmitHeaderPresent; // заголовок принимаемого сообщения был прочитан
    private boolean receiveHeaderPresent; // в отправляемое сообщение был записан заголовок

    //private ByteBuffer data;        // emulating data(file) needed to be transferred to client

    private Path receiveFilePath;     // путь к файлу для приема

    private long remainingBytesToRead;
    //private ByteArrayOutputStream bufferStream; // работает с readBuffer при приеме текстового сообщения

    // хотя можно просто унаследоваться от SeekableByteChannel и сделать два Channel
    // TextChannel и BinaryChannel - в одном текст, в другом - файл
    //private MessageType receiveMessageType;    // received message type



    public Connection() {

        bufferedTransmitChannel = new SeekableInMemoryByteChannel();
        bufferedReceiveChannel = new SeekableInMemoryByteChannel();

        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    }

    public Connection(SelectionKey key, Instant time) {
        this();

        this.key = key;
        this.channel = (SocketChannel)key.channel();
        this.time = time;
    }

    public Connection(SocketChannel channel) {
        this();

        this.channel = channel;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public Instant getTime() {return time;}

    public void setTime(Instant time) {this.time = time;}

/*    public RandomAccessFile getFile() {return file;}

    public void setFile(RandomAccessFile file) {this.file = file;}*/

    public SeekableByteChannel getTransmitChannel() {
        return transmitChannel;
    }

    public void setTransmitChannel(SeekableByteChannel transmitChannel) {
        this.transmitChannel = transmitChannel;
    }

    public SeekableByteChannel getReceiveChannel() {
        return receiveChannel;
    }

    public void setReceiveChannel(SeekableByteChannel receiveChannel) {
        this.receiveChannel = receiveChannel;
    }

    public boolean isTransmitHeaderPresent() {
        return transmitHeaderPresent;
    }

    public void setTransmitHeaderPresent(boolean transmitHeaderPresent) {
        this.transmitHeaderPresent = transmitHeaderPresent;
    }


    public boolean isReceiveHeaderPresent() {
        return receiveHeaderPresent;
    }

    public void setReceiveHeaderPresent(boolean receiveHeaderPresent) {
        this.receiveHeaderPresent = receiveHeaderPresent;
    }

    public long remainingBytesToRead() {
        return remainingBytesToRead;
    }

    public void remainingBytesToRead(long remainingBytesToRead) {
        this.remainingBytesToRead = remainingBytesToRead;
    }

    public void decreaseRemainingBytesToRead(long amount) {
        remainingBytesToRead-= amount;
    }

    public Path getReceiveFilePath() {
        return receiveFilePath;
    }

    public void setReceiveFilePath(Path receiveFilePath) {
        this.receiveFilePath = receiveFilePath;
    }

    public SeekableByteChannel getBufferedTransmitChannel() {

        try {
            bufferedTransmitChannel.position(0);
            bufferedTransmitChannel.truncate(0);

            // Теперь transmitChannel ссылается на bufferedTransmitChannel
            //transmitChannel = bufferedTransmitChannel;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return bufferedTransmitChannel;
    }

//    public void setBufferedTransmitChannel(SeekableByteChannel bufferedTransmitChannel) {
//        this.bufferedTransmitChannel = bufferedTransmitChannel;
//    }

    public SeekableByteChannel getBufferedReceiveChannel() {

        try {
            bufferedReceiveChannel.position(0);
            bufferedReceiveChannel.truncate(0);

            // Теперь receiveChannel ссылается на bufferedReceiveChannel
            //receiveChannel = bufferedReceiveChannel;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return bufferedReceiveChannel;
    }

//    public void setBufferedReceiveChannel(SeekableByteChannel bufferedReceiveChannel) {
//        this.bufferedReceiveChannel = bufferedReceiveChannel;
//    }

    //    public MessageType getReceiveMessageType() {return receiveMessageType;}
//
//    public void setReceiveMessageType(MessageType receiveMessageType) {
//        this.receiveMessageType = receiveMessageType;
//    }


    // -----------------------------------------------------------------------------------

    public void close() {

        // close socket
        if (channel != null &&
            channel.isOpen()) {

            try {
                channel.close();
            } catch (IOException ignored) {}
        }

        // close transmitChannel
        if (transmitChannel != null) {
            try {
                transmitChannel.close();
            } catch (IOException ignored) {}
        }

        // close receiveChannel
        if (receiveChannel != null) {
            try {
                receiveChannel.close();
            } catch (IOException ignored) {}
        }
    }



    /**
     * Write header to connection.writeBuffer
     */
    public void writeHeader() {

        try {

            assert writeBuffer.position() == 0;

            // write message size
            writeBuffer.putLong(transmitChannel.size());

            // write message type
            writeBuffer.put(getChannelType(transmitChannel).getValue());

            setTransmitHeaderPresent(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Read header from connection.readBuffer
     */
    public MessageType parseHeader() {

        MessageType result = null;

        try {

            // return to beginning of buffer
            readBuffer.rewind();

            // get payload length
            remainingBytesToRead = readBuffer.getLong();

            // позволим дальше писать в буфер
            //readBuffer.limit(readBuffer.capacity());

            // get message type
            result = MessageType.parse(readBuffer.get());


            // увеличим количество байт для чтения на размер заголовка (8+1)
            // т.к. в цикле readSocket(..)
            // в количество прочитанных байт из сокета 'read' вошли как длина заголовока,
            // так и число прочтенных байт самого сообщения, поэтому возвращаем обратно
            // (помечаем ка кнепрочитанные)
            remainingBytesToRead += (8 + 1);  // int64 - message size + 1 byte message type

            setReceiveHeaderPresent(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    // =====================================================================


    public MessageType getChannelType(SeekableByteChannel channel) {

        MessageType result = null;

        if (channel instanceof SeekableInMemoryByteChannel) {
            result = MessageType.TEXT;
        }
        else if (channel instanceof FileChannel) {
            result = MessageType.BINARY;
        }

        return result;
    }



    public FileChannel createFileChannel(Path path, String mode) {

        FileChannel result = null;
        try {

            RandomAccessFile file = new RandomAccessFile(path.toString(), mode);
            result = file.getChannel();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }





}
