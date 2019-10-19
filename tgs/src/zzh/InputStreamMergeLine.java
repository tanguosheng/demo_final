package zzh;

import java.util.concurrent.LinkedBlockingQueue;

public class InputStreamMergeLine extends Thread {

    private byte[] left = new byte[0];

    private Boolean finish = false;

    private LinkedBlockingQueue<byte[]> cacheQueue;

    public InputStreamMergeLine(Boolean finish, LinkedBlockingQueue<byte[]> cacheQueue) {
        this.finish = finish;
        this.cacheQueue = cacheQueue;
    }

    public void run() {
        
    }
}
