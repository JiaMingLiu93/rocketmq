/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.simple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 1. 把broker模块src/test/resources/META-INF/service/org.apache.rocketmq.acl.AccessValidator 复制到src/java/resources/META-INF/service
 * 2. 查看distribution模块下 /conf/transport.yml文件，注意里面的账户密码，ip
 * 3. 把ALC_RCP_HOOK_ACCOUT与ACL_RCP_HOOK_PASSWORD 修改成transport.yml里面对应的账户密码
 * @author laohu
 *
 */
public class AclClient {
	
	private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

	private static String ALC_RCP_HOOK_ACCOUT = "RocketMQ";
	 
	private static String ACL_RCP_HOOK_PASSWORD = "1234567";
	 
	
	
	 public static void main(String[] args) throws MQClientException, InterruptedException {
		 producer();
		 pushConsumer();
		 pullConsumer();
	 }
	 
	 public static void producer() throws MQClientException {
		 DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName",getAalRPCHook());
		 producer.setNamesrvAddr("127.0.0.1:9876");
	        producer.start();

	        for (int i = 0; i < 128; i++)
	            try {
	                {
	                    Message msg = new Message("TopicTest",
	                        "TagA",
	                        "OrderID188",
	                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
	                    SendResult sendResult = producer.send(msg);
	                    System.out.printf("%s%n", sendResult);
	                }

	            } catch (Exception e) {
	                e.printStackTrace();
	            }

	        producer.shutdown();
	 }
	 
	 public static void pushConsumer() throws MQClientException {

	        
	        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_5" , getAalRPCHook(),new AllocateMessageQueueAveragely());
	        consumer.setNamesrvAddr("127.0.0.1:9876");
	        consumer.subscribe("TopicTest", "*");
	        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
	        //wrong time format 2017_0422_221800
	        consumer.setConsumeTimestamp("20180422221800");
	        consumer.registerMessageListener(new MessageListenerConcurrently() {

	            @Override
	            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
	                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
	                printBody(msgs);
	                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	            }
	        });
	        consumer.start();
	        System.out.printf("Consumer Started.%n");
	 }
	 
	 public static void pullConsumer() throws MQClientException {
		 DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_6" , getAalRPCHook());
	        consumer.setNamesrvAddr("127.0.0.1:9876");
	        consumer.start();

	        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
	        for (MessageQueue mq : mqs) {
	            System.out.printf("Consume from the queue: %s%n", mq);
	            SINGLE_MQ:
	            while (true) {
	                try {
	                    PullResult pullResult =
	                        consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
	                    System.out.printf("%s%n", pullResult);
	                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
	                    printBody(pullResult);
	                    switch (pullResult.getPullStatus()) {
	                        case FOUND:
	                            break;
	                        case NO_MATCHED_MSG:
	                            break;
	                        case NO_NEW_MSG:
	                            break SINGLE_MQ;
	                        case OFFSET_ILLEGAL:
	                            break;
	                        default:
	                            break;
	                    }
	                } catch (Exception e) {
	                    e.printStackTrace();
	                }
	            }
	        }

	        consumer.shutdown();
	 }
	 
	 private static void printBody(PullResult pullResult) {
		 printBody(pullResult.getMsgFoundList());
	 }
	 
	 private static void printBody(List<MessageExt> msg) {
		 if(msg == null || msg.size() == 0)
			 return;
		 for(MessageExt m : msg) {
			 if(m != null) {
				 System.out.printf("msgId : %s  body : %s",m.getMsgId() , new String(m.getBody()));
				 System.out.println();
			 }
		 }
	 }
	 
	 private static long getMessageQueueOffset(MessageQueue mq) {
	        Long offset = OFFSE_TABLE.get(mq);
	        if (offset != null)
	            return offset;

	        return 0;
	    }

	    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
	        OFFSE_TABLE.put(mq, offset);
	    }
	 
	 static    RPCHook getAalRPCHook() {
		 return new AalRPCHook(ALC_RCP_HOOK_ACCOUT, ACL_RCP_HOOK_PASSWORD);
	 }
	    
	    
	 static class AalRPCHook implements RPCHook{

		 private String account;
		 
		 private String password;
		 
		 public AalRPCHook(String account , String password) {
			this.account = account;
			this.password = password;
		}
		 
		@Override
		public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
			
			HashMap<String, String> ext = request.getExtFields();
			if(ext == null) {
				ext = new HashMap<>();
				request.setExtFields(ext);
			}
			ext.put("account", this.account);
			ext.put("password", this.password);
		}

		@Override
		public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
			// TODO Auto-generated method stub
			
		}
		 
	 }
}
