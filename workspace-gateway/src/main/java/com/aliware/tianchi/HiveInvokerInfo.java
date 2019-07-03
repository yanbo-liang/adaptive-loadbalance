package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HiveInvokerInfo {
    volatile String name;
    volatile Invoker invoker;

    volatile long maxRequest = 0;
    AtomicLong currentRequest = new AtomicLong(0);

    long[] rttCache = new long[20];
    AtomicInteger rttCacheIndex = new AtomicInteger(-1);

    public HiveInvokerInfo(Invoker invoker) {
        String host = invoker.getUrl().getHost();
        int start = host.indexOf('-');
        this.name = host.substring(start + 1);
        this.invoker = invoker;
    }
}
