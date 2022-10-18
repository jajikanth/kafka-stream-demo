package com.jajikanth.kafkastreamdemo;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.jajikanth.kafkastreamdemo.joiner.CustomerBalanceJoiner;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class StreamsJoinDemo {
    public static void main(String[] args) throws IOException {

        KafkaStreams kafkaStreams = null;
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
//----------Set required properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-basic");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");


        StreamsBuilder builder = new StreamsBuilder();
        final String customerTopic = "Customer";
        final String balanceTopic = "Balance";
        final String outputTopic = "CustomerBalance";


        //-------------------BEGIN------------------

        Map<String, Object> configMap = propertiesToMap(props);
//-------define SerDes
        SpecificAvroSerde<Customer> customerSerDe = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<Balance> balanceSerDes = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<CustomerBalance> combinedSerde = getSpecificAvroSerde(configMap);

///----------Define kTables---------
        final KTable<Long, Customer> customerKStream = builder.table(customerTopic, Consumed.with(Serdes.Long(), customerSerDe));
        final KTable<Long, Balance> balanceKStream = builder.table(balanceTopic, Consumed.with(Serdes.Long(), balanceSerDes));


        //---define joiner
        final CustomerBalanceJoiner customerBalanceJoiner = new CustomerBalanceJoiner();
        //-------do inner join
        final KTable<Long, CustomerBalance> customerBalanceKStream = balanceKStream.join(customerKStream, customerBalanceJoiner::apply);
        //-----send combined data to new topic
        customerBalanceKStream.toStream().to(outputTopic, Produced.with(Serdes.Long(), combinedSerde));

        kafkaStreams.start();
        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
        System.out.println("**************************************kstreams starting on customer");
    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }


    public static Map<String, Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String) key, (String) value));
        return configs;
    }

}
