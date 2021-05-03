# zeek-conn-aggregator-kafka-streams

*_input_* - a stream of records like this from the `conn` topic:
 
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


*_output_* - aggregates of bytes, packets, connection counts, etc... between pairs of hosts, in 5-minute windows, produced to the `conn-5-minute-aggregation` topic:

    {
        "orig_bytes":141,
        "resp_bytes":311,
        "orig_pkts":3,
        "orig_ip_bytes":225,
        "resp_pkts":3,
        "resp_ip_bytes":395,
        "missed_bytes":0,
        "connection_count":3,
        "id_orig_h":"10.0.1.36",
        "id_resp_h":"10.0.1.1",
        "windowStart":1620072900000,
        "windowEnd":1620073200000
    }

