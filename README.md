# zeek-conn-aggregator-kafka-streams

* ###### input - a stream of records like this:


    {
        "ts":1619562183740,
        "uid":"Ct6k6d2TnyF7Zp0Gs",
        "id_orig_h":"10.0.1.41",
        "id_orig_p":3,
        "id_resp_h":"10.0.1.16",
        "id_resp_p":10,
        "proto":"icmp",
        "conn_state":"OTH",
        "local_orig":true,
        "local_resp":true,
        "missed_bytes":0,
        "orig_pkts":1,
        "orig_ip_bytes":106,
        "resp_pkts":0,
        "resp_ip_bytes":0
    }


* output - counts of the number of connections, between pairs of hosts, in 5-minute windows:


    {"id_orig_h":"10.0.1.33","id_resp_h":"10.0.1.1","windowStart":1619562300000,"windowEnd":1619562600000,"connection_count":6}
    {"id_orig_h":"10.0.1.16","id_resp_h":"10.0.1.32","windowStart":1619562300000,"windowEnd":1619562600000,"connection_count":1}
    {"id_orig_h":"10.0.1.40","id_resp_h":"10.0.1.41","windowStart":1619562300000,"windowEnd":1619562600000,"connection_count":25}
    {"id_orig_h":"10.0.1.40","id_resp_h":"10.0.1.42","windowStart":1619562300000,"windowEnd":1619562600000,"connection_count":27}
    {"id_orig_h":"10.0.1.29","id_resp_h":"255.255.255.255","windowStart":1619562300000,"windowEnd":1619562600000,"connection_count":1}

