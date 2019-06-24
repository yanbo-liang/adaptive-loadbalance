package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {

    public volatile static Map<String, AtomicLong> rttMap = new ConcurrentHashMap<>();


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            String key = invoker.getUrl().toString();

            if (rttMap.get(key) == null) {
                synchronized (TestClientFilter.class) {
                    if (rttMap.get(key) == null) {
                        rttMap.put(key, new AtomicLong(-1));
                    }
                }
            }

            long start = System.nanoTime();
            Result result = invoker.invoke(invocation);
            long end = System.nanoTime();
            long rtt = end - start;

            long value = rttMap.get(key).get();
            if (value > 0 && rtt > value * 2) {
                UserLoadBalance.blockMap.compute(key, (k, v) -> {
                    if (v == null) {
                        return new AtomicInteger(1);
                    } else {
                        v.updateAndGet(x -> x + 1);
                        return v;
                    }
                });
            }

            rttMap.compute(key, (k, v) -> {
                v.accumulateAndGet(rtt, (old, param) -> {
                    if (old > 0) {
                        return (long) (0.7 * old + 0.3 * param);
                    } else {
                        return param;
                    }
                });
                return v;

            });
            return result;

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }
}
