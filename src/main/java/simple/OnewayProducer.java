package simple;

import common.Constant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author FatShallot
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 创建对象
        DefaultMQProducer producer = new DefaultMQProducer("simple_group");
        producer.setNamesrvAddr(Constant.NAME_SERVER_ADDRESS);
        // 启动
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("simple_topic", "simple_tag_oneway", String.valueOf(i).getBytes());
            producer.sendOneway(message);
        }
        producer.shutdown();
    }
}
