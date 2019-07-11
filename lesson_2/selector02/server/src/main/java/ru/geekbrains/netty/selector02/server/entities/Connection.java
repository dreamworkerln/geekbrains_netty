package ru.geekbrains.netty.selector02.server.entities;

import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Instant;

public class Connection {

    private SelectionKey key;
    private SocketChannel channel;
    private ByteBuffer readBuffer;  // буффер на чтение
    private ByteBuffer writeBuffer; // буффер на запись
    private Instant time;
    private ByteBuffer data;        // emulating data(file) needed to be transferred to client

    private RandomAccessFile file; // работает с readBuffer/writeBuffer при передаче файла
    private ByteArrayOutputStream bufferStream; // работает с readBuffer при приеме текстового сообщения


    public Connection(SelectionKey key, Instant time) {

        this.key = key;
        this.channel = (SocketChannel)key.channel();
        this.readBuffer = ByteBuffer.allocate(ConnectionList.BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocate(ConnectionList.BUFFER_SIZE);
        this.bufferStream = new ByteArrayOutputStream(ConnectionList.BUFFER_SIZE);
        this.time = time;
        this.data = null;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public ByteArrayOutputStream getBufferStream() {
        return bufferStream;
    }


    public Instant getTime() {return time;}

    public void setTime(Instant time) {this.time = time;}

    public RandomAccessFile getFile() {return file;}
}
