package transaction;

import common.Constant;
import order.OrderStep;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

public class TransactionProducer {
    public static void main(String[] args)
            throws MQClientException {
        //Instantiate with a producer group name.
        TransactionMQProducer producer = new TransactionMQProducer("transaction_group");
        producer.setNamesrvAddr(Constant.NAME_SERVER_ADDRESS);
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 根据本地事务的执行结果决定是否提交
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                String tags = message.getTags();
                int integer = Integer.parseInt(tags);
                if (integer == 0) {
                    return LocalTransactionState.UNKNOW;
                } else if (integer % 2 == 1) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (integer % 2 == 0) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else {
                    return LocalTransactionState.UNKNOW;
                }
            }

            /**
             * 对半事务的状态进行回查
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                String tags = messageExt.getTags();
                int integer = Integer.parseInt(tags);
                if (integer == 0) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }
        });
        //Launch the instance.
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("transaction_topic", String.valueOf(i), String.valueOf(i).getBytes());
            producer.sendMessageInTransaction(message, null);
        }
        //server shutdown
        producer.shutdown();
    }
}
