package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicLong;

public class HiveInvokerInfo {
    String name;
    Invoker invoker;

    //    volatile double maxRequestCoefficient = 1;
    AtomicLong pendingRequest = new AtomicLong(0);
    volatile int maxPendingRequest = 0;


    AtomicLong totalTime = new AtomicLong(0);
    AtomicLong totalRequest = new AtomicLong(0);
    volatile double rttAverage = 0;

    volatile double weight = 0;
    volatile double weightBound = 0;
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
                ", maxPendingRequest=" + maxPendingRequest +
                ", pendingRequest=" + pendingRequest +
                ", totalTime=" + totalTime +
                ", totalRequest=" + totalRequest +
                ", rttAverage=" + rttAverage +
                ", weight=" + weight +
                ", weightBound=" + weightBound +
                '}';
    }
}
