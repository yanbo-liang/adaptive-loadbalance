package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;

@Activate(group = Constants.CONSUMER)
public class HiveFilter implements Filter {
    public static final ConcurrentMap<Invocation, Long> rttMap = new ConcurrentReferenceHashMap<>(1024, ConcurrentReferenceHashMap.ReferenceType.WEAK);

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
            hiveInvokerInfo.rtt.updateAndGet(x -> {
                if (x == 0) {
                    return rtt;
                } else {
                    return (long) (x * 0.8 + rtt * 0.3);
                }
            });
        }
        return result;
    }
}
