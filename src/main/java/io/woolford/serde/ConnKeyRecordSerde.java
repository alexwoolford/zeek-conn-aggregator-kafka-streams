package io.woolford.serde;

import io.woolford.ConnKeyRecord;
import org.apache.kafka.common.serialization.Serdes;

public class ConnKeyRecordSerde extends Serdes.WrapperSerde<ConnKeyRecord> {
    public ConnKeyRecordSerde() {
        super(new JsonSerializer(), new JsonDeserializer(ConnKeyRecord.class));
    }
}
