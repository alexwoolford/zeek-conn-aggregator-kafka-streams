package io.woolford.serde;

import io.woolford.ConnWindowCountRecord;
import org.apache.kafka.common.serialization.Serdes;

public class ConnWindowCountSerde extends Serdes.WrapperSerde<ConnWindowCountRecord> {
    public ConnWindowCountSerde() {
        super(new JsonSerializer(), new JsonDeserializer(ConnWindowCountRecord.class));
    }
}

