# Benchmark Results

These results were generated from the completed benchmark runner and published as interview evidence.

## Scenario A

1 producer, 1 consumer, 10,000 messages, 4 partitions

- Throughput: 329.9977 msg/sec
- Average producer latency: 2.0890 ms
- Producer P50: 1.9537 ms
- Producer P95: 2.4952 ms
- Producer P99: 6.4671 ms
- Average end-to-end latency: 2420.6091 ms
- End-to-end P50: 2399.6572 ms
- End-to-end P95: 4540.4070 ms
- End-to-end P99: 4773.0264 ms
- Consumer lag: 2542

Partition distribution:

- P0: 2475
- P1: 2484
- P2: 2542
- P3: 2499

## Scenario B

5 producers, 5 consumers, 50,000 messages, 8 partitions

- Throughput: 107.3367 msg/sec
- Average producer latency: 4.0039 ms
- Producer P50: 3.4125 ms
- Producer P95: 7.0560 ms
- Producer P99: 11.4333 ms
- Average end-to-end latency: 102259.3130 ms
- End-to-end P50: 41913.6133 ms
- End-to-end P95: 344143.6732 ms
- End-to-end P99: 374308.3267 ms
- Consumer lag: 6433

Partition distribution:

- P0: 6216
- P1: 6212
- P2: 6220
- P3: 6091
- P4: 6358
- P5: 6203
- P6: 6267
- P7: 6433

## Scenario C

10 producers, 20 consumers, 100,000 messages, 16 partitions

- Throughput: 338.0421 msg/sec
- Average producer latency: 6.2131 ms
- Producer P50: 5.2489 ms
- Producer P95: 11.6243 ms
- Producer P99: 17.3928 ms
- Average end-to-end latency: 107193.5278 ms
- End-to-end P50: 107020.0047 ms
- End-to-end P95: 182941.8358 ms
- End-to-end P99: 194486.5853 ms
- Consumer lag: 6335

Partition distribution:

- P0: 6272
- P1: 6324
- P2: 6199
- P3: 6212
- P4: 6296
- P5: 6266
- P6: 6299
- P7: 6065
- P8: 6240
- P9: 6335
- P10: 6300
- P11: 6272
- P12: 6270
- P13: 6149
- P14: 6267
- P15: 6234

## Scenario D

20 producers, 20 consumers, 100,000 messages, 16 partitions

- Throughput: 334.3722 msg/sec
- Average producer latency: 11.8935 ms
- Producer P50: 9.3264 ms
- Producer P95: 26.4784 ms
- Producer P99: 44.0393 ms
- Average end-to-end latency: 117321.0464 ms
- End-to-end P50: 117845.9068 ms
- End-to-end P95: 197199.3105 ms
- End-to-end P99: 211971.5587 ms
- Consumer lag: 6358

Partition distribution:

- P0: 6333
- P1: 6231
- P2: 6287
- P3: 6130
- P4: 6228
- P5: 6215
- P6: 6208
- P7: 6313
- P8: 6330
- P9: 6124
- P10: 6185
- P11: 6358
- P12: 6347
- P13: 6207
- P14: 6269
- P15: 6235

