package common;

public class Buffer {
    private byte[] bufferArray;
    private int index = -1;

    public Buffer(int size) {
        bufferArray = new byte[size];
    }

    public void add(byte b) {
        bufferArray[++index] = b;
    }

    public void appendArray(byte[] src, int srcPos, int length) {
        System.arraycopy(src, srcPos, bufferArray, index + 1, length);
        index += length;
    }

    public void copy(byte[] target, int destPos) {
        System.arraycopy(bufferArray, 0, target, destPos, index + 1);
    }

    public byte[] getByteArray() {
        byte[] result = new byte[index + 1];
        System.arraycopy(bufferArray, 0, result, 0, index + 1);
        return result;
    }

    public void reset() {
        index = -1;
    }

    public int size() {
        return index + 1;
    }

    @Override
    public String toString() {
        byte[] result = new byte[index + 1];
        System.arraycopy(bufferArray, 0, result, 0, index + 1);
        return new String(result);
    }
}
