package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {
    static final Semaphore rttSemaphore = new Semaphore(500, true);

    static final ConcurrentMap<Invocation, Long> rttMap = new ConcurrentReferenceHashMap<>(1024, ConcurrentReferenceHashMap.ReferenceType.WEAK);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            rttMap.put(invocation, System.currentTimeMillis());
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        if (result.hasException()) {
            if (result.getException().getMessage().contains("EXHAUSTED")) {
                HiveInvokerInfo hiveInvokerInfo = UserLoadBalance.infoMap.get(invoker.getUrl());
                hiveInvokerInfo.exhausted = true;
                return result;
            }
        }

        Long start = rttMap.get(invocation);
        if (start != null) {
            long rtt = System.currentTimeMillis() - start;
            HiveInvokerInfo hiveInvokerInfo = UserLoadBalance.infoMap.get(invoker.getUrl());
            try {
                rttSemaphore.acquire();
                hiveInvokerInfo.totalRtt.updateAndGet(x -> x + rtt);
                hiveInvokerInfo.totalRequest.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                rttSemaphore.release();
            }
        }
        return result;
    }
}
