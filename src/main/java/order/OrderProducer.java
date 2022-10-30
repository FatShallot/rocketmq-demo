package order;

import common.Constant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

public class OrderProducer {
    public static void main(String[] args)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("order_group");
        producer.setNamesrvAddr(Constant.NAME_SERVER_ADDRESS);
        //Launch the instance.
        producer.start();
        List<OrderStep> orderSteps = OrderStep.buildOrders();
        for (int i = 0; i < orderSteps.size(); i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("order_topic", "oder_topic", "key" + i,
                    (orderSteps.get(i).toString()).getBytes());
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    long id = (long) arg;
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, orderSteps.get(i).getOrderId());
            // orderSteps.get(i).getOrderId()会被arg接收

            System.out.printf("%s%n", sendResult);
        }
        //server shutdown
        producer.shutdown();
    }

    public static void main1(String[] args)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("wangyan_test");
        producer.setNamesrvAddr("10.1.13.189:9876;10.1.16.189:9876;10.1.14.197:9876;10.1.16.157:9876");
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 1; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("wangyan_test", "wangyan_tag", "key" + i,
                    "wangyan_message".getBytes());
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    long id = (long) arg;
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, 123L);
            // orderSteps.get(i).getOrderId()会被arg接收

            System.out.printf("%s%n", sendResult);
        }
        //server shutdown
        producer.shutdown();
    }
}
