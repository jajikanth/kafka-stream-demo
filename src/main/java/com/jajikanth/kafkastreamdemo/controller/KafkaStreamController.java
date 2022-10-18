package com.jajikanth.kafkastreamdemo.controller;

import com.ibm.gbs.schema.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
@Slf4j
public class KafkaStreamController {

    private KafkaStreams kafkaStreams;

    @GetMapping("/begin-stream")
    public void beginStreaming(){
        if(kafkaStreams != null){
            kafkaStreams.close();
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-outer-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        //  props.setProperty("key.deserializer", StringDeserializer.class.getName());
        //  props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("specific.avro.reader", "true");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Customer> customerStream = builder.stream("Customer");

        customerStream.print(Printed.toSysOut());

        customerStream.foreach((key, value) -> log.info("key:" + key + ", value:" + value));

        // start the stream
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        log.info("kstreams starting on customer");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
