package com.jasongj.kafka.stream;

import com.jasongj.kafka.stream.model.Model1;
import com.jasongj.kafka.stream.model.Model2;
import com.jasongj.kafka.stream.serdes.SerdesFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;

import java.io.IOException;
import java.util.Properties;

public class TestJoin {
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-test-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka0:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper0:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder streamBuilder = new KStreamBuilder();
        KStream<String, Model1> model1Stream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Model1.class), "topic1");
        KTable<String, Model2> model2Stream = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Model2.class), "topic5","model2-state-store");

        //model1Stream.foreach((str, str2) -> System.out.printf("model1Stream: %s-%s\n", str, str2));
        //model2Stream.foreach((str, str2) -> System.out.printf("model2Stream: %s-%s\n", str, str2));

        KStream<String, String> kStream = model1Stream
            .leftJoin(model2Stream, (Model1 m1, Model2 m2) ->ModelCat.fromModel2(m1, m2) , Serdes.String(), SerdesFactory.serdFrom(Model1.class))
            .filter((String k, ModelCat model12) -> model12.v2 != null)
            .map((String k, ModelCat model12) -> new KeyValue<>(k, String.format("{}/{}", model12.v1, model12.v2)));
        //kStream.foreach((str, str2) -> System.out.printf("%s-%s\n", str, str2));
        kStream.to("topic4");

        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.in.read();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }

    public static class ModelCat {
        private String k;
        private String v1;
        private String v2;

        public static ModelCat fromModel1(Model1 m1) {
            ModelCat model12 = new ModelCat();
            if(m1 == null) {
                return model12;
            }
            model12.k = m1.getK();
            model12.v1 = m1.getV();
            return model12;
        }

        public static ModelCat fromModel2(Model1 m1, Model2 m2) {
            ModelCat model12 = fromModel1(m1);
            if (m2 == null) {
                return model12;
            }
            model12.v2 = m2.getV();
            return model12;
        }
    }
}
