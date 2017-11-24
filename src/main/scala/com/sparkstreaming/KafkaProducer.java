package com.sparkstreaming;

import com.alibaba.fastjson.JSON;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducer {

	private final AtomicLong index = new AtomicLong(0);
	private Producer<String, byte[]> producer;
	private Properties props = new Properties();
	private String brokerList;
	//sync同步 async异步
	private String producerType = "async";
	
	public void init(){
		if(StringUtils.isBlank(brokerList)){
			throw new IllegalArgumentException("brokerList不能为null");
		}
		props.put("metadata.broker.list", brokerList);
		props.put("producer.type", producerType);
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "-1");
		props.put("retry.backoff.ms", "12000");
	
		producer = new Producer<String, byte[]>(new ProducerConfig(props));
	}
	public void shutdown(){
		if(this.producer != null){
			this.producer.close();
		}
	}
	
	public void send(String topic, Object message){
		byte[] body = JSON.toJSONBytes(message);
		KeyedMessage<String, byte[]> keyedMessage = new KeyedMessage<String, byte[]>(topic, key(), body);
		producer.send(keyedMessage);
	}
	private String key(){
		return String.valueOf(index.getAndIncrement());
	}

	public String getBrokerList() {
		return brokerList;
	}
	public void setBrokerList(String brokerList) {
		this.brokerList = brokerList;
	}
	public String getProducerType() {
		return producerType;
	}
	public void setProducerType(String producerType) {
		this.producerType = producerType;
	}
	
}