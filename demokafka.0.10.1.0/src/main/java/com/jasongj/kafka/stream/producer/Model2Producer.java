package com.jasongj.kafka.stream.producer;

import com.jasongj.kafka.producer.HashPartitioner;
import com.jasongj.kafka.stream.model.Model2;
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

public class Model2Producer {
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
		props.put("value.serializer.type", Model2.class.getName());
		props.put("partitioner.class", HashPartitioner.class.getName());

		Producer<String, Model2> producer = new KafkaProducer<String, Model2>(props);
		List<Model2> model2s = readModel();
		model2s.forEach((Model2 model2) -> producer.send(new ProducerRecord<String, Model2>("topic5", model2.getK(), model2)));
		producer.close();
	}
	
	public static List<Model2> readModel() throws IOException {
		InputStream inputStream = Model2Producer.class.getResourceAsStream("/model2.csv");
		List<String> lines = IOUtils.readLines(inputStream, Charset.forName("UTF-8"));
		List<Model2> model2s = lines.stream()
			.filter(StringUtils::isNoneBlank)
			.map((String line) -> line.split("\\s*,\\s*"))
				.filter((String[] values) -> values.length == 2)
			.map((String[] values) -> new Model2(values[0], values[1]))
			.collect(Collectors.toList());
		return model2s;
	}

}
