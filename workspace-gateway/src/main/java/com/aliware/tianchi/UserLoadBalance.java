package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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


    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        Set<String> blockString = new HashSet<>();
        Test.block.forEachEntry(10, stringIntegerEntry -> {
            Integer a = stringIntegerEntry.getValue();
            if (a != null && a > 0) {
                blockString.add(stringIntegerEntry.getKey());
            }
        });


        int index = ThreadLocalRandom.current().nextInt(invokers.size());
        Invoker<T> invoker = invokers.get(index);
        if (blockString.contains(invoker.getUrl().toString())) {

            Test.block.compute(invoker.getUrl().toString(), (k, v) -> v = v - 1);
            for (Invoker<T> invoker1 : invokers) {
                if (!invoker1.getUrl().toString().equals(invoker.getUrl().toString())) {
                    return invoker1;
                }
            }

            return null;
        } else {
            return invoker;
        }

//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}
