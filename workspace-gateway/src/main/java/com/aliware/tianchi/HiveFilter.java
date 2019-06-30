package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {
    static final Semaphore rttSemaphore = new Semaphore(100, true);

    static final ConcurrentMap<Invocation, Long> rttMap = new ConcurrentReferenceHashMap<>(2048, ConcurrentReferenceHashMap.ReferenceType.WEAK);


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            HiveInvokerInfo hiveInvokerInfo = UserLoadBalance.infoMap.get(invoker.getUrl());
            if (hiveInvokerInfo != null) {
                hiveInvokerInfo.currentRequest.incrementAndGet();
                rttMap.put(invocation, System.currentTimeMillis());
            }
            return invoker.invoke(invocation);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        HiveInvokerInfo hiveInvokerInfo = UserLoadBalance.infoMap.get(invoker.getUrl());
        if (hiveInvokerInfo != null) {
            hiveInvokerInfo.currentRequest.decrementAndGet();
            Long start = rttMap.get(invocation);
            if (start != null) {
                long rtt = System.currentTimeMillis() - start;
                try {
                    rttSemaphore.acquire();
                    hiveInvokerInfo.totalRtt.updateAndGet(x -> x + rtt);
                    hiveInvokerInfo.totalRequest.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    rttSemaphore.release();
                }
                int index = hiveInvokerInfo.rttCacheIndex.updateAndGet(x -> {
                    if (x < 99) {
                        return x + 1;
                    } else {
                        return 0;
                    }
                });
                hiveInvokerInfo.rttCache[index] = rtt;
            }
        }
        return result;
    }
}
