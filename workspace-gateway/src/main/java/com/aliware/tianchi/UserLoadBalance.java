package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author daofeng.xjf
 * <p>
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    private static final ThreadLocal<Boolean> threadLocal = new ThreadLocal<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {

        Invoker defaultInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        String defaultInvokerKey = defaultInvoker.getUrl().toString();

        threadLocal.set(false);
        TestClientFilter.blockMap.get(defaultInvokerKey).updateAndGet(x -> {
            if (x > 0) {
                threadLocal.set(true);
                return x - 1;
            } else {
                return x;
            }
        });
        if (threadLocal.get()) {
            defaultInvoker = null;
            for (Invoker<T> invoker : invokers) {
                String newKey = invoker.getUrl().toString();
                if (!newKey.equals(defaultInvokerKey)) {
                    AtomicInteger atomicInteger = TestClientFilter.blockMap.get(newKey);
                    if (atomicInteger != null && atomicInteger.get() == 0) {
                        defaultInvoker = invoker;
                    }

                }
            }
        }


        if (defaultInvoker == null) {
            throw new RpcException("too busy");
        } else {
            return defaultInvoker;
        }
//return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}
