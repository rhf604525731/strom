package test.kafka;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.utils.Utils;

public class Producer extends Thread {
	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public Producer(String topic) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");// 瀛楃涓叉秷鎭�
		props.put("metadata.broker.list","172.16.65.25:9092,172.16.65.26:9092,172.16.65.27:9092");
		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
		this.topic = topic;
	}

	public void run() {
	
		int i =0 ;
		while(true) {
			i ++ ;
			String messageStr = "test" + i;
			System.out.println("product:"+messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			Utils.sleep(1000) ;
		}

	}

	public static void main(String[] args) {
		Producer producerThread = new Producer("topic1");
		producerThread.start();
	}
}
