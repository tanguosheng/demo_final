package zzh;

public class Bytes {

    private byte[] key;

    public Bytes(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        } else if (!(obj instanceof Bytes)) {
            return false;
        }
        Bytes mo = (Bytes) obj;

        byte[] other = mo.getKey();

        if (other.length != key.length) {
            return false;
        }

        for (int i = 0; i < key.length; i++) {
            if (other[i] != key[i]) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 1;
        for (byte b : key)
            h = 31 * h + b;
        return h;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }
}
