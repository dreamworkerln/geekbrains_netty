package ru.geekbrains.netty.selector03.common.entities;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.time.Instant;

public class Connection {

    private static final int BUFFER_SIZE = 15; // read and write buffer size


    public enum MessageType {
        TEXT(0),
        BINARY(1);

        private int value;


        MessageType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static MessageType parse(int value) {

            MessageType result = null;


            if (value == 0) {
                result = Connection.MessageType.TEXT;
            }
            else if (value == 1) {
                result = Connection.MessageType.BINARY;
            }

            return result;
        }
    }


    private SelectionKey key;
    private SocketChannel channel;  // сокет
    private ByteBuffer readBuffer;  // промежуточный буффер на чтение
    private ByteBuffer writeBuffer; // промежуточный буффер на запись
    private Instant time;
    private SeekableByteChannel transmitChannel;  // канал на передачу данных клиенту
    private SeekableByteChannel receiveChannel;   // канал на прием данных от клиента

    private boolean headerHasReaded; // заголовок принимаемого сообщения был прочитан
    private boolean headerHasWrited; // в отправляемое сообщение был записан заголовок

    //private ByteBuffer data;        // emulating data(file) needed to be transferred to client

    //private RandomAccessFile file; // работает с readBuffer/writeBuffer (замещая receiveChannel/transmitChannel)
    private long remainingBytesToRead;
    //private ByteArrayOutputStream bufferStream; // работает с readBuffer при приеме текстового сообщения

    // server state (text/binary)
    private MessageType messageType;    // current messageType



    public Connection(SelectionKey key, Instant time) {

        this.key = key;
        this.channel = (SocketChannel)key.channel();
        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.messageType = MessageType.TEXT;
        this.time = time;
    }

    public Connection(SocketChannel channel) {

        this.channel = channel;
        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.messageType = MessageType.TEXT;
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

    public boolean isHeaderHasReaded() {
        return headerHasReaded;
    }

    public void setHeaderHasReaded(boolean headerHasReaded) {
        this.headerHasReaded = headerHasReaded;
    }


    public boolean isHeaderHasWrited() {
        return headerHasWrited;
    }

    public void setHeaderHasWrited(boolean headerHasWrited) {
        this.headerHasWrited = headerHasWrited;
    }

    public long getRemainingBytesToRead() {
        return remainingBytesToRead;
    }

    public void setRemainingBytesToRead(long remainingBytesToRead) {
        this.remainingBytesToRead = remainingBytesToRead;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }


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

            ByteBuffer buffer = getWriteBuffer();
            assert buffer.position() == 0;

            // write message size
            buffer.putLong(getTransmitChannel().size());

            // write message type
            buffer.put((byte)getMessageType().getValue());

            setHeaderHasWrited(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * Read header from connection.readBuffer
     */
    public void parseHeader() {

        try {

            ByteBuffer buffer = getReadBuffer();

            buffer.rewind();

            // get payload length
            setRemainingBytesToRead(buffer.getLong());

            // увеличим количество байт для чтения на размер заголовка (8+1)
            // т.к. в цикле readSocket(..)
            // В количество прочитанных байт из сокета 'read' войдет как длина заголовока,
            // так и число прочтенных байт самого сообщения
            setRemainingBytesToRead(getRemainingBytesToRead() + 8 + 1);

            // get message type
            setMessageType(Connection.MessageType.parse(buffer.get()));

            setHeaderHasReaded(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




}
