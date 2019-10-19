package zzh;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MapHolder {
	static ThreadLocal<Map<String, AtomicInteger>> domainHolder = new ThreadLocal<>();
	static ThreadLocal<Map<String, AtomicInteger>> apiHolder = new ThreadLocal<>();

	public static void setDomainMap(Map<String, AtomicInteger> map) {
		domainHolder.set(map);
	}

	public static Map<String, AtomicInteger> getDomainMap() {
		return domainHolder.get();
	}

	public static void setApiMap(Map<String, AtomicInteger> map) {
		apiHolder.set(map);
	}

	public static Map<String, AtomicInteger> getApiMap() {
		return apiHolder.get();
	}
}
