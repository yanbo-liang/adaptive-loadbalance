package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HiveInvokerInfo {
    volatile String name;
    volatile Invoker invoker;

    volatile long maxRequest = 0;
    AtomicLong currentRequest = new AtomicLong(0);

    static int length = 30;
    long[] rttCache = new long[length];
    AtomicInteger rttCacheIndex = new AtomicInteger(-1);
     final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public HiveInvokerInfo(Invoker invoker) {
        String host = invoker.getUrl().getHost();
        int start = host.indexOf('-');
        this.name = host.substring(start + 1);
        this.invoker = invoker;
    }
}
