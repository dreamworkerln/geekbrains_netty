package ru.geekbrains.netty.selector03.server.entities;

import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;

public class Connection {


    public enum Mode {
        TEXT,
        BINARY
    }


    private SelectionKey key;
    private SocketChannel channel;
    private ByteBuffer readBuffer;  // промежуточный буффер на чтение
    private ByteBuffer writeBuffer; // промежуточный буффер на запись
    private Instant time;
    private ReadableByteChannel readChannel;  // источник данных от клиента
    private WritableByteChannel writeChannel; // приемник данных которые передаем клиенту

    private boolean headerReceived;

    //private ByteBuffer data;        // emulating data(file) needed to be transferred to client

    private RandomAccessFile file; // работает с readBuffer/writeBuffer (замещая writeChannel/readChannel)
    //private ByteArrayOutputStream bufferStream; // работает с readBuffer при приеме текстового сообщения

    // server state (text/binary)
    //private Mode currentMode;  // current mode
    //private Mode nextMode;     // desirable mode on next select loop iteration




    public Connection(SelectionKey key, Instant time) {

        this.key = key;
        this.channel = (SocketChannel)key.channel();
        this.readBuffer = ByteBuffer.allocate(ConnectionList.BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocate(ConnectionList.BUFFER_SIZE);
        //this.bufferStream = new ByteArrayOutputStream(ConnectionList.BUFFER_SIZE);
        this.time = time;
        //this.data = null;

        //this.currentMode = Mode.TEXT;
        //this.nextMode = null;
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

    public RandomAccessFile getFile() {return file;}

    public void setFile(RandomAccessFile file) {this.file = file;}

    public ReadableByteChannel getReadChannel() {
        return readChannel;
    }

    public void setReadChannel(ReadableByteChannel readChannel) {
        this.readChannel = readChannel;
    }

    public WritableByteChannel getWriteChannel() {
        return writeChannel;
    }

    public void setWriteChannel(WritableByteChannel writeChannel) {
        this.writeChannel = writeChannel;
    }

    public boolean isHeaderReceived() {
        return headerReceived;
    }

    public void setHeaderReceived(boolean headerReceived) {
        this.headerReceived = headerReceived;
    }

    /*
    public ByteArrayOutputStream getBufferStream() {
        return bufferStream;
    }

    public Mode getCurrentMode() {return currentMode;}

    public void setCurrentMode(Mode currentMode) {this.currentMode = currentMode;}

    public Mode getNextMode() {return nextMode;}

    public void setNextMode(Mode nextMode) {this.nextMode = nextMode;}

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }
    */

}
