package io.woolford;

import lombok.Data;

@Data
public class ConnWindowCountRecord {

    String id_orig_h;
    String id_resp_h;
    long windowStart;
    long windowEnd;
    long connection_count;

}
