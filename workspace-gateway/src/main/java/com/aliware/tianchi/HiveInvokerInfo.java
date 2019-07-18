package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HiveInvokerInfo {
    String name;
    Invoker invoker;

    volatile int maxPendingRequest = 0;
    AtomicInteger pendingRequest = new AtomicInteger(0);

    volatile int rtt = 0;

    volatile int totalTime = 0;
    volatile int totalRequest = 0;

    volatile int maxConcurrency = 0;

    volatile double weight = 0;
    volatile double weightInitial = 0;
    volatile double currentWeight = 0;
    volatile double weightTop = 0;

    ReadWriteLock lock = new ReentrantReadWriteLock();

    public HiveInvokerInfo(Invoker invoker) {
        String host = invoker.getUrl().getHost();
        int start = host.indexOf('-');
        this.name = host.substring(start + 1);
        this.invoker = invoker;
    }


}
