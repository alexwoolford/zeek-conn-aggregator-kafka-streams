package io.woolford.serde;

import io.woolford.AggregateCountRecord;
import org.apache.kafka.common.serialization.Serdes;

public class AggregateRecordCountSerde extends Serdes.WrapperSerde<AggregateCountRecord> {
    public AggregateRecordCountSerde() {
        super(new JsonSerializer(), new JsonDeserializer(AggregateCountRecord.class));
    }
}
