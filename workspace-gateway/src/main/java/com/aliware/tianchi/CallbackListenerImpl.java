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
        int index = msg.indexOf('-');
        if (index != -1) {
            String name = msg.substring(0, index);
            long maxRequest = Long.valueOf(msg.substring(index + 1));
            UserLoadBalance.infoMap.forEach((k, v) -> {
                if (v.name.equals(name)) {
                    v.maxRequest = maxRequest;
                }
            });
        }

//        System.out.println("receive msg from server :" + msg);
    }
}