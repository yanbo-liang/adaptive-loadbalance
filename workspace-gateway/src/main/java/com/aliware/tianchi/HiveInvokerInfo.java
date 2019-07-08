package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HiveInvokerInfo {

    volatile String name;
    volatile Invoker invoker;

    volatile double maxRequestCoefficient = 0.5;
    volatile int maxRequest = 0;
    AtomicLong pendingRequest = new AtomicLong(0);

    volatile AtomicLong rttTotalTime = new AtomicLong(0);
    volatile AtomicLong rttTotalCount = new AtomicLong(0);
    volatile double rttAverage = 0;


//    volatile double stressCoefficient = 0.5;
    volatile double weight=0;
    volatile double weightBound = 0;
//    long[] rttCache = new long[20];
//    AtomicInteger rttCacheIndex = new AtomicInteger(-1);
//    final ReadWriteLock lock = new ReentrantReadWriteLock();

    public HiveInvokerInfo(Invoker invoker) {
        String host = invoker.getUrl().getHost();
        int start = host.indexOf('-');
        this.name = host.substring(start + 1);
        this.invoker = invoker;
    }

    @Override
    public String toString() {
        return "HiveInvokerInfo{" +
                "name='" + name + '\'' +
                ", invoker=" + invoker +
                ", maxRequest=" + maxRequest +
                ", pendingRequest=" + pendingRequest +
                ", rttTotalTime=" + rttTotalTime +
                ", rttTotalCount=" + rttTotalCount +
                ", weight=" + weight +
                ", weightBound=" + weightBound +
                '}';
    }
}
