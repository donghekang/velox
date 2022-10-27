1. ReadRel:
    1.1 set the path by uri_file
    1.2 Filter only support int and double
    1.3 filter must be conjunctive
    1.4 each filter must be a scalar function
    1.5 not support boolean

2. ProjectRel:
    2.1 it first removes all fields and then produces new fields
    2.2 not support binary, should use string/varchar

3. AggregateRel:
    3.1 The first columns in the result are the groupby keys

4. ExchangeRel:
    4.1 The partition_count in the rel is ignored. The number of partitions is
        determined at runtime based on the parallelism of the upperstream pipeline