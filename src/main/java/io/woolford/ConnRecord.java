package io.woolford;

import lombok.Data;

import java.util.Date;

@Data
public class ConnRecord {

    Date ts;
    String uid;
    String id_orig_h;
    String id_orig_p;
    String id_resp_h;
    String id_resp_p;
    String proto;
    String service;
    Double duration;
    Long orig_bytes;
    Long resp_bytes;
    String conn_state;
    Boolean local_orig;
    Boolean local_resp;
    Long missed_bytes;
    String history;
    Long orig_pkts;
    Long orig_ip_bytes;
    Long resp_pkts;
    Long resp_ip_bytes;

}
