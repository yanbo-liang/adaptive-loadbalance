package com.aliware.tianchi;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HiveInvokerInfo {
    volatile int weight = 100;
    volatile int rttWeight = 0;
    AtomicLong totalRtt = new AtomicLong(0);
    AtomicLong totalRequest = new AtomicLong(0);
    long averageRtt = Long.MAX_VALUE;
    volatile boolean exhausted = false;
    AtomicLong lastChangeTime = new AtomicLong(0);

    @Override
    public String toString() {
        return "HiveInvokerInfo{" +
                "weight=" + weight +
                ", rttWeight=" + rttWeight +
                ", totalRtt=" + totalRtt +
                ", totalRequest=" + totalRequest +
                ", averageRtt=" + averageRtt +
                ", exhausted=" + exhausted +
                ", lastChangeTime=" + lastChangeTime +
                '}';
    }
}
