package com.jasongj.kafka.stream.producer;

import com.jasongj.kafka.producer.HashPartitioner;
import com.jasongj.kafka.stream.model.Model1;
import com.jasongj.kafka.stream.serdes.GenericSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Model1Producer {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka0:9092");
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", GenericSerializer.class.getName());
		props.put("value.serializer.type", Model1.class.getName());
		props.put("partitioner.class", HashPartitioner.class.getName());

		Producer<String, Model1> producer = new KafkaProducer<String, Model1>(props);
		List<Model1> model1s = readModel();
		model1s.forEach((Model1 model1) -> producer.send(new ProducerRecord<String, Model1>("topic1", model1.getK(), model1)));
		producer.close();
	}
	
	public static List<Model1> readModel() throws IOException {
		InputStream inputStream = Model1Producer.class.getResourceAsStream("/model1.csv");
		List<String> lines = IOUtils.readLines(inputStream, Charset.forName("UTF-8"));
		List<Model1> model1s = lines.stream()
			.filter(StringUtils::isNoneBlank)
			.map((String line) -> line.split("\\s*,\\s*"))
			.filter((String[] values) -> values.length == 2)
			.map((String[] values) -> new Model1(values[0], values[1]))
			.collect(Collectors.toList());
		return model1s;
	}

}
