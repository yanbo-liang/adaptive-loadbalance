package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {
    static final ConcurrentMap<Invocation, Long> rttMap = new ConcurrentReferenceHashMap<>(2000, ConcurrentReferenceHashMap.ReferenceType.WEAK);
    static final Semaphore semaphore = new Semaphore(100);

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
        try {
            HiveInvokerInfo hiveInvokerInfo = UserLoadBalance.infoMap.get(invoker.getUrl());
            if (hiveInvokerInfo != null) {
                hiveInvokerInfo.currentRequest.decrementAndGet();
                Long start = rttMap.get(invocation);
                if (start != null) {

                    long rtt = System.currentTimeMillis() - start;

                    hiveInvokerInfo.maxRtt.updateAndGet(x -> {
                        if (rtt > x) {
                            return rtt;
                        } else {
                            return x;
                        }
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return result;
    }
}
