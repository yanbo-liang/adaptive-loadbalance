package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicLong;

public class HiveInvokerInfo {
    AtomicLong totalRtt = new AtomicLong(0);
    AtomicLong totalRequest = new AtomicLong(0);
    long averageRtt = Long.MAX_VALUE;

    volatile long maxRequest = -1;
    AtomicLong currentRequest = new AtomicLong(0);

    volatile String name;
    volatile Invoker invoker;

    public HiveInvokerInfo(Invoker invoker) {
        String host = invoker.getUrl().getHost();
        int start = host.indexOf('-');
        this.name = host.substring(start + 1);
        this.invoker = invoker;
    }
}
