package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HiveInvokerInfo {
    String name;
    Invoker invoker;

    AtomicLong pendingRequest = new AtomicLong(0);
    volatile int maxPendingRequest = 0;

    AtomicLong totalTime = new AtomicLong(0);
    AtomicLong totalRequest = new AtomicLong(0);

    volatile double rttAverage = 0;

    volatile double weight = 0;
    volatile double weightInitial = 0;
    volatile double currentWeight = 0;
    volatile double weightTop = 0;

    volatile boolean smallest = false;
    ReadWriteLock lock = new ReentrantReadWriteLock();


    public HiveInvokerInfo(Invoker invoker) {
        String host = invoker.getUrl().getHost();
        int start = host.indexOf('-');
        this.name = host.substring(start + 1);
        this.invoker = invoker;
    }

    @Override
    public String toString() {
        return "name='" + name + '\'' +
                ", pendingRequest=" + pendingRequest +
                ", totalTime=" + totalTime +
                ", totalRequest=" + totalRequest +
                ", rttAverage=" + rttAverage +
                ", weight=" + weight +
                ", weightTop=" + weightTop +
                '}';
    }

//    volatile double maxRequestCoefficient = 1;

}
