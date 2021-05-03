package io.woolford;

import io.woolford.serde.AggregateRecordCountSerde;
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
        AggregateRecordCountSerde aggregateRecordCountSerde = new AggregateRecordCountSerde();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, connKeyRecordSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, connRecordSerde.getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ConnTimestampExtractor.class);

        final StreamsBuilder builder = new StreamsBuilder();

        // re-key stream of Zeek conn records so they're grouped by orig/resp pairs
        builder.stream(
                "conn",                /* input topic */
                Consumed.with(
                        connKeyRecordSerde,  /* key serde   */
                        connRecordSerde      /* value serde */
                        )
                ).selectKey((key, value) -> {
                    ConnKeyRecord connKeyRecord = new ConnKeyRecord();
                    connKeyRecord.id_orig_h = value.id_orig_h;
                    connKeyRecord.id_resp_h = value.id_resp_h;
                    return connKeyRecord;
                }).through("conn-keyed-for-aggregation");

        // bucket records into 5-minute tumbling windows
        TimeWindowedKStream<ConnKeyRecord, ConnRecord> connTimeWindowed =
                builder.stream("conn-keyed-for-aggregation", Consumed.with(connKeyRecordSerde, connRecordSerde))
                        .groupByKey(Grouped.with(connKeyRecordSerde, connRecordSerde))
                        .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)));

        // aggregate the bytes, packets, counts, etc...
        connTimeWindowed.aggregate(

                () -> {AggregateCountRecord aggregateCountRecord = new AggregateCountRecord();
                    aggregateCountRecord.setOrig_bytes(0L);
                    aggregateCountRecord.setResp_bytes(0L);
                    aggregateCountRecord.setConnection_count(0L);
                    return aggregateCountRecord;},

                (aggKey, newValue, aggValue) -> {

                    if (newValue.orig_bytes != null) {
                        aggValue.orig_bytes = aggValue.orig_bytes + newValue.orig_bytes;
                    }

                    if (newValue.resp_bytes != null) {
                        aggValue.resp_bytes = aggValue.resp_bytes + newValue.resp_bytes;
                    }

                    if (newValue.orig_pkts != null) {
                        aggValue.orig_pkts = aggValue.orig_pkts + newValue.orig_pkts;
                    }

                    if (newValue.orig_ip_bytes != null) {
                        aggValue.orig_ip_bytes = aggValue.orig_ip_bytes + newValue.orig_ip_bytes;
                    }

                    if (newValue.resp_pkts != null) {
                        aggValue.resp_pkts = aggValue.resp_pkts + newValue.resp_pkts;
                    }

                    if (newValue.resp_ip_bytes != null) {
                        aggValue.resp_ip_bytes = aggValue.resp_ip_bytes + newValue.resp_ip_bytes;
                    }

                    if (newValue.missed_bytes != null) {
                        aggValue.missed_bytes = aggValue.missed_bytes + newValue.missed_bytes;
                    }

                    aggValue.setConnection_count(aggValue.connection_count + 1L);

                    return aggValue;
                },

                Materialized.with(connKeyRecordSerde, aggregateRecordCountSerde)

        ).toStream().map((key, value) -> {

            ConnWindowCountRecord connWindowCountRecord = new ConnWindowCountRecord();
            connWindowCountRecord.windowStart = key.window().start();
            connWindowCountRecord.windowEnd = key.window().end();
            connWindowCountRecord.id_orig_h = (key.key().id_orig_h);
            connWindowCountRecord.id_resp_h = (key.key().id_resp_h);
            connWindowCountRecord.orig_bytes = value.orig_bytes;
            connWindowCountRecord.resp_bytes = value.resp_bytes;
            connWindowCountRecord.orig_pkts = value.orig_pkts;
            connWindowCountRecord.orig_ip_bytes = value.orig_ip_bytes;
            connWindowCountRecord.resp_pkts = value.resp_pkts;
            connWindowCountRecord.resp_ip_bytes = value.resp_ip_bytes;
            connWindowCountRecord.missed_bytes = value.missed_bytes;
            connWindowCountRecord.connection_count = value.connection_count;

            return new KeyValue<>(null, connWindowCountRecord);

        }).to("conn-5-minute-aggregation", Produced.valueSerde(connWindowCountSerde));

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
