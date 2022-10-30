package order;

import common.Constant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class OrderConsumer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_group");
        consumer.setNamesrvAddr(Constant.NAME_SERVER_ADDRESS);
        consumer.subscribe("order_topic", "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.print(Thread.currentThread().getName());
                    System.out.print(" ");
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }

    /**
     * 消费者的测试必须在main里面运行，单测的话方法消费完存量消息就结束了
     */
    public static void main1(String[] args) throws MQClientException, InterruptedException {
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_wangyan_test");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_wangyan_test");
        consumer.setNamesrvAddr("10.1.13.189:9876;10.1.16.189:9876;10.1.14.197:9876;10.1.16.157:9876");
        String sql = "APP_ENV_NAME='test'";
        consumer.subscribe("wangyan_test", MessageSelector.bySql(sql));
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.print(Thread.currentThread().getName());
                    System.out.print(" ");
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }

}
