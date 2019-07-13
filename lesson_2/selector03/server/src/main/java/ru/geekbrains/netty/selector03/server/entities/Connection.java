package ru.geekbrains.netty.selector03.server.entities;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.time.Instant;

public class Connection {

    private static final int BUFFER_SIZE = 10; // read and write buffer size


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
    private SocketChannel channel;
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
        this.receiveChannel = new SeekableInMemoryByteChannel(); // preinit receive
        this.time = time;
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

    public void close() {


        // close socket
        if (channel != null &&
            channel.isOpen()) {

            try {
                channel.close();
            } catch (IOException ignored) {}
        }

//        // close file
//        if (file != null) {
//            try {
//                file.close();
//            } catch (IOException ignored) {}
//        }


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


    /*
    public ByteArrayOutputStream getBufferStream() {
        return bufferStream;
    }

    public MessageType getCurrentMode() {return currentMode;}

    public void setCurrentMode(MessageType currentMode) {this.currentMode = currentMode;}

    public MessageType getNextMode() {return nextMode;}

    public void setNextMode(MessageType nextMode) {this.nextMode = nextMode;}

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }
    */

}
