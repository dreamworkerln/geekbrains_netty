package ru.geekbrains.netty.selector03.common.entities;

public enum MessageType {
    TEXT((byte)0),
    BINARY((byte)1);

    private byte value;


    MessageType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static MessageType parse(byte value) {

        MessageType result = null;


        if (value == 0) {
            result = MessageType.TEXT;
        }
        else if (value == 1) {
            result = MessageType.BINARY;
        }
        return result;
    }


}
