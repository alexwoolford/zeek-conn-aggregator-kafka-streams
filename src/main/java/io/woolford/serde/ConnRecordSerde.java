package io.woolford.serde;

import io.woolford.ConnRecord;
import org.apache.kafka.common.serialization.Serdes;

public class ConnRecordSerde extends Serdes.WrapperSerde<ConnRecord> {
    public ConnRecordSerde() {
        super(new JsonSerializer(), new JsonDeserializer(ConnRecord.class));
    }
}
