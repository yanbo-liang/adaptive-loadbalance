package com.aliware.tianchi;


import org.apache.dubbo.rpc.listener.CallbackListener;

/**
 * @author daofeng.xjf
 * <p>
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 */
public class CallbackListenerImpl implements CallbackListener {

    @Override
    public void receiveServerMsg(String msg) {
        String[] split = msg.split("-");
        for (HiveInvokerInfo info : HiveCommon.infoMap.values()) {
            if (info.name.equals(split[0])) {
                HiveCommon.lock.readLock().lock();
                info.maxPendingRequest = Integer.valueOf(split[1]);

                info.maxConcurrency = Integer.valueOf(split[2]);

                info.totalTime = Integer.valueOf(split[3]);
                info.totalRequest = Integer.valueOf(split[4]);
                if (info.totalRequest != 0) {
                    info.rtt = info.totalTime / info.totalRequest;
                }
                HiveCommon.lock.readLock().unlock();

                break;
            }
        }

        HiveCommon.initCallBack();
//        System.out.println("receive msg from server :" + msg);
    }
}