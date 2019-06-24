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

    public static Map<String, Long> rttMap =new ConcurrentHashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        for (Invoker<T> invoker : invokers) {
            if (rttMap.get(invoker.getUrl().toString()) == null) {
                return invoker;
            }
        }
        long rttMax = Long.MAX_VALUE;
        Invoker pickedInvoker = invokers.get(0);
        for (Invoker<T> invoker : invokers) {
            long rtt = rttMap.get(invoker.getUrl().toString());
            if (rtt < rttMax) {
                rttMax = rtt;
                pickedInvoker = invoker;
            }
        }

        return pickedInvoker;
//        int index = ThreadLocalRandom.current().nextInt(invokers.size());
//        Invoker<T> invoker = invokers.get(index);

//        String key = invoker.getUrl().toString();
//
//        if (Test.block.getOrDefault(key, 0) == 0) {
//            return invoker;
//        } else {
//            Test.block.compute(key, (k, v) -> {
//                if (v != null) {
//                    return v - 1;
//                } else {
//                    return null;
//                }
//            });
//
//
//            for (Invoker<T> inv : invokers) {
//                String newKey = inv.getUrl().toString();
//                if (!newKey.equals(key)) {
//                    if (Test.block.getOrDefault(newKey, 0) == 0) {
//                        return inv;
//                    }
//                }
//            }
//        }
//        return null;
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}
