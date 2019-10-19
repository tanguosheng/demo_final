package zzh;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {
	static int COUNT_BITS = Integer.SIZE - 3;
	static int CAPACITY = (1 << COUNT_BITS) - 1;

	public static void main(String[] args) {
		/*
		 * int RUNNING = -1 << COUNT_BITS; int SHUTDOWN = 0 << COUNT_BITS; int STOP = 1
		 * << COUNT_BITS; int TIDYING = 2 << COUNT_BITS; int TERMINATED = 3 <<
		 * COUNT_BITS; AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));
		 * 
		 * byte[] left = new byte[0];
		 * 
		 * System.out.println(CAPACITY); System.out.println(RUNNING);
		 * System.out.println(SHUTDOWN); System.out.println(STOP);
		 * System.out.println(TIDYING); System.out.println(TERMINATED);
		 * System.out.println(ctl.get()); System.out.println(workerCountOf(ctl.get()));
		 * 
		 * System.out.println(left.length);
		 */

		/*byte[] buff = new byte[10];
		for (int i = 0; i < buff.length; i++) {
			if(i < 5) {
				buff[i] = 1;
			} else {
				buff[i] = 2;
			}
			if (i == 5) {
				buff[i] = '\n';
			}
			
		}

		for (int i = 0; i < buff.length; i++) {
			if (buff[i] == '\n') {
				byte[] qbyte = new byte[i + 1];
				System.arraycopy(buff, 0, qbyte, 0, i + 1);
				int leftLen = buff.length - i - 1;
				byte[] hbyte = new byte[leftLen];
				System.arraycopy(buff, 0, qbyte, 0, i + 1);
				System.arraycopy(buff, i + 1, hbyte, 0, leftLen);
				System.out.println("aaa");
			}
		}*/
		
		String str = "	? ";
		byte[] buff =  str.getBytes();
		System.out.println("aa");
	}

	private static int ctlOf(int rs, int wc) {
		return rs | wc;
	}

	private static int workerCountOf(int c) {
		return c & CAPACITY;
	}
}
