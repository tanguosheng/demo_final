package zzh;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InputStreamTask_blb implements Runnable {
	private static final String TAB = "\t";
	private static final String SPACE = " ";
	private static final String WENHAO = "?";

	private byte[] buff;
	private ConcurrentLinkedQueue<Map<String, AtomicInteger>> domainQueue;
	private ConcurrentLinkedQueue<Map<String, AtomicInteger>> apiQueue;
	private ConcurrentHashMap<String, AtomicInteger> stMap;

	InputStreamTask_blb(byte[] buff, ConcurrentLinkedQueue<Map<String, AtomicInteger>> domainQueue,
			ConcurrentLinkedQueue<Map<String, AtomicInteger>> apiQueue,
			ConcurrentHashMap<String, AtomicInteger> stMap) {
		this.buff = buff;
		this.domainQueue = domainQueue;
		this.apiQueue = apiQueue;
		this.stMap = stMap;
	}

	@Override
	public void run() {
		Map<String, AtomicInteger> domainMap = domainQueue.poll();
		Map<String, AtomicInteger> apiMap = apiQueue.poll();

		String s = "";
		try {
			String[] strs = new String(buff).split("\n");

			for (String line : strs) {
				s = line;
				if (line == null || line.isEmpty()) {
					continue;
				}
				int lineIndex3 = indexOf(line, TAB, 3);
				int lineIndex4 = indexOf(line, TAB, 4);
				String str = line.substring(lineIndex3 + 1, lineIndex4);
				String domain = str.substring(0, str.indexOf(SPACE));
				 AtomicInteger domainInteger = stMap.computeIfAbsent(domain, k -> new
				 AtomicInteger(0));
				 domainInteger.incrementAndGet();

				int strIndex5 = indexOf(str, SPACE, 5);
				int strIndex6 = indexOf(str, SPACE, 6);
				String url = str.substring(strIndex5 + 1, strIndex6);
				if (url.indexOf(WENHAO) > 0) {
					url = url.substring(0, url.indexOf(WENHAO));
				}
				 AtomicInteger urlInteger = apiMap.computeIfAbsent(url, k -> new
				 AtomicInteger(0));
				 urlInteger.incrementAndGet();
			}
		} catch (Exception e) {
			System.out.println(s);
		} finally {
			domainQueue.add(domainMap);
			apiQueue.add(apiMap);
		}
	}

	private static int indexOf(String targetStr, String flag, int time) {
		if (time <= 0) {
			return -1;
		}
		int index = -1;
		for (int i = 1; i <= time; i++) {
			index = targetStr.indexOf(flag, index + 1);
		}
		return index;
	}
}
