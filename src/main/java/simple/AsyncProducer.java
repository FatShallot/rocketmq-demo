package simple;

import common.Constant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * @author FatShallot
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建对象
        DefaultMQProducer producer = new DefaultMQProducer("simple_group");
        producer.setNamesrvAddr(Constant.NAME_SERVER_ADDRESS);
        // 启动
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("simple_topic", "simple_tag_async", String.valueOf(i).getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println(throwable);
                }
            });
        }
        producer.shutdown();
    }
}
