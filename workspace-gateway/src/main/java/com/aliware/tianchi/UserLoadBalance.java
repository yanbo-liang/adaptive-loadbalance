package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author daofeng.xjf
 * <p>
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    private static final Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final AtomicBoolean inited = new AtomicBoolean(false);

    public static final ConcurrentMap<String, Integer> weightMap = new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {

        int totalWeight = 0;
        for (Invoker<T> invoker : invokers) {
            String key = invoker.getUrl().toString();
            Integer weight = weightMap.get(key);
            if (weight == null) {
                synchronized (weightMap) {
                    if (weightMap.get(key) == null) {
                        weightMap.put(key, 100);
                    }
                }
                weight = weightMap.get(key);
            }
            totalWeight += weight;
        }

        if (!inited.get()) {
            if (inited.compareAndSet(false, true)) {
                Task task = new Task();
                executorService.execute(task);
            }
        }

        int[] section = new int[invokers.size()];
        int subTotal = 0;
        for (int i = 0; i < invokers.size(); i++) {
            String key = invokers.get(i).getUrl().toString();
            subTotal += weightMap.get(key);
            section[i] = subTotal;
        }
        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        for (int i = 0; i < section.length; i++) {
            if (random < section[i]) {
                return invokers.get(i);
            }
        }

        System.exit(1);
        return null;
    }
//    @Override
//    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        if (!inited.get()) {
//            if (inited.compareAndSet(false,true)){
//                Task task = new Task();
//                executorService.execute(task);
//            }
//        }
//
//
//        for (Invoker<T> invoker : invokers) {
//            String key = invoker.getUrl().toString();
//
//            AtomicInteger atomicInteger = TestClientFilter.totalMap.get(key);
//            if (atomicInteger != null) {
//            }
//
//        }
//
//
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//    }

//    @Override
//    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//
//        Invoker defaultInvoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//        String defaultInvokerKey = defaultInvoker.getUrl().toString();
//
//        threadLocal.set(false);
//        AtomicInteger blockcount = TestClientFilter.blockMap.get(defaultInvokerKey);
//        if (blockcount != null) {
//            blockcount.updateAndGet(x -> {
//                if (x > 0) {
//                    threadLocal.set(true);
//                    return x - 1;
//                } else {
//                    return x;
//                }
//            });
//        }
//
//        if (threadLocal.get()) {
//            defaultInvoker = null;
//            for (Invoker<T> invoker : invokers) {
//                String newKey = invoker.getUrl().toString();
//                if (!newKey.equals(defaultInvokerKey)) {
//                    AtomicInteger atomicInteger = TestClientFilter.blockMap.get(newKey);
//                    if (atomicInteger != null && atomicInteger.get() == 0) {
//                        defaultInvoker = invoker;
//                    }
//
//                }
//            }
//        }
//
//
//        if (defaultInvoker == null) {
//            throw new RpcException("too busy");
//        } else {
//            return defaultInvoker;
//        }
//    }
}
