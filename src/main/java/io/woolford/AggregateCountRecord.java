package io.woolford;

import lombok.Data;

@Data
public class AggregateCountRecord {

    long orig_bytes;
    long resp_bytes;
    long orig_pkts;
    long orig_ip_bytes;
    long resp_pkts;
    long resp_ip_bytes;
    long missed_bytes;
    long connection_count;

}
