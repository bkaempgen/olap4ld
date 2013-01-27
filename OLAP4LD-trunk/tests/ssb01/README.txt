==Original==
* lineorder.tbl (6001171 rows) - SSB1 (not included)
* lineorder.tbl (60176 rows) - SSB001
* date.tbl (2556 rows) - SSB1
* customer.tbl (30000 rows) - SSB1
* part.tbl (200000 rows) - SSB1
* supplier.tbl (2000 rows) - SSB1

==TTL==
(triple count is line count)
* lineorder.ttl (114022252 triples) - SSB1 (not included)
* lineorder.ttl (1143347 triples) - SSB001
* date.ttl (48567 triples) - SSB1
* customer.ttl (300003 triples) - SSB1
* part.ttl (2200003 triples) - SSB1
* supplier.ttl (18003 triples) - SSB1

==QB TTL==
(triple count is line count)
* lineorder_qb.ttl (114022252 + 2 * 6001171 = 126024594 triples) - SSB1 + QB Observation/Data set Triples (not included)
* lineorder_qb.ttl (1143347 + 2 * 60176 = 1263699 triples) - SSB001
* date_dimension_transitive.ttl (19239 triples) - SSB1
* customer_dimension_transitive.ttl (? triples) - SSB1
* part_dimension_transitive.ttl (1004083 triples, 36544 msec) - SSB1
* supplier_dimension_transitive.ttl (11083 triples) - SSB1
