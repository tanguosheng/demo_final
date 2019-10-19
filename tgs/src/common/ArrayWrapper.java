package common;

import java.util.Arrays;

public class ArrayWrapper {
    byte[] array;

    private int hash;

    private int count = 0;

    public ArrayWrapper(byte[] array) {
        this.array = array;
    }

    public byte[] getArray() {
        return array;
    }

    public int getCount() {
        return count;
    }

    public int increment() {
        return ++count;
    }

    @Override
    public String toString() {
        return new String(array);
    }

    //    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        ArrayWrapper that = (ArrayWrapper) o;
//        return Arrays.equals(array, that.array);
//    }

    /**
     * 不标准的equals，比标准的少两个步骤
     */
    @Override
    public boolean equals(Object o) {
        return Arrays.equals(array, ((ArrayWrapper) o).array);
    }


    @Override
    public int hashCode() {
        int h = 1;
        for (byte b : array)
            h = 31 * h + b;
        return h;
    }

//    @Override
//    public int hashCode() {
//        int h = hash;
//        if (array != null && h == 0 && array.length > 0) {
//            byte[] val = array;
//            for (int i = 0; i < array.length; i++) {
//                h = 31 * h + val[i];
//            }
//            hash = h;
//        }
//        return h;
//    }
}
