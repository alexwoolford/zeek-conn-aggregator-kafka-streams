package io.woolford;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class ConnTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        ConnRecord connRecord = (ConnRecord) record.value();
        return connRecord.ts.getTime();
    }

}