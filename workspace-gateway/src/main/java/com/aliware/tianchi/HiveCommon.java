package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class HiveCommon {
    static final ConcurrentMap<URL, HiveInvokerInfo> infoMap = new ConcurrentHashMap<>();
    static final ConcurrentMap<Invocation, Long> rttMap = new ConcurrentReferenceHashMap<>(2000, ConcurrentReferenceHashMap.ReferenceType.SOFT);
    static final AtomicInteger pendingRequestTotal = new AtomicInteger(0);
    static volatile List<HiveInvokerInfo> infoList;
    static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    static final SimpleDateFormat format = new SimpleDateFormat("mm:ss:SSS");


}
