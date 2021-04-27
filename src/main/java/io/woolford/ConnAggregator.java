package io.woolford;

import io.woolford.serde.ConnKeyRecordSerde;
import io.woolford.serde.ConnRecordSerde;
import io.woolford.serde.ConnWindowCountSerde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ConnAggregator {

    final Logger LOG = LoggerFactory.getLogger(ConnAggregator.class);

    void run() throws IOException {

        // load properties
        Properties props = new Properties();
        InputStream input = ConnAggregator.class.getClassLoader().getResourceAsStream("config.properties");
        props.load(input);

        ConnRecordSerde connRecordSerde = new ConnRecordSerde();
        ConnKeyRecordSerde connKeyRecordSerde = new ConnKeyRecordSerde();
        ConnWindowCountSerde connWindowCountSerde = new ConnWindowCountSerde();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, connKeyRecordSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, connRecordSerde.getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ConnTimestampExtractor.class);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<ConnKeyRecord, ConnRecord> connRecordKStream = builder.stream(
                "conn",                /* input topic */
                Consumed.with(
                        connKeyRecordSerde,  /* key serde   */
                        connRecordSerde      /* value serde */
                        )
                ).selectKey((key, value) -> {
                    ConnKeyRecord connKeyRecord = new ConnKeyRecord();
                    connKeyRecord.setId_orig_h(value.getId_orig_h());
                    connKeyRecord.setId_resp_h(value.getId_resp_h());
                    return connKeyRecord;
                }).through("conn-keyed-for-aggregation");

        TimeWindowedKStream<ConnKeyRecord, ConnRecord> connTimeWindowed =
                builder.stream("conn-keyed-for-aggregation", Consumed.with(connKeyRecordSerde, connRecordSerde))
                        .groupByKey()
                        .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)));

        connTimeWindowed.count().toStream().map((key, value) -> {

            ConnWindowCountRecord connWindowCountRecord = new ConnWindowCountRecord();
            connWindowCountRecord.setWindowStart(key.window().start());
            connWindowCountRecord.setWindowEnd(key.window().end());
            connWindowCountRecord.setId_orig_h(key.key().getId_orig_h());
            connWindowCountRecord.setId_resp_h(key.key().getId_resp_h());
            connWindowCountRecord.setConnection_count(value);

            return new KeyValue<>(null, connWindowCountRecord);

        }).to("conn-count", Produced.valueSerde(connWindowCountSerde));

        // run it
        final Topology topology = builder.build();

        // show topology
        LOG.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}

