package com.aliware.tianchi;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HiveInvokerInfo {
    AtomicInteger weight = new AtomicInteger(100);
    AtomicLong rtt = new AtomicLong(0);
    volatile boolean exhausted = false;
}
