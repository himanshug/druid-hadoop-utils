set druid.overlord.hostport localhost:${port}

define hyperUniqueAggregator com.yahoo.druid.pig.udfs.NonFinalizingAggregatorFactoryAdapter('{"type": "hyperUnique","name": "unique_hosts","fieldName": "unique_hosts"}', 'hyperUnique');
define hyperUniquePostAgg com.yahoo.druid.pig.udfs.DoublePostAggregatorAdapter('{"type": "hyperUniqueCardinality","name": "unique_hosts","fieldName": "unique_hosts"}', '[{"type": "hyperUnique","name": "unique_hosts"}]');

A = load 'testDatasource' using com.yahoo.druid.pig.DruidStorage('sample-schema.json','1970-01-01T00:00:00.000/3000-01-01T00:00:00.000');
describe A;

B = group A by (druid_timestamp, host);
describe B;

C = foreach B generate group, SUM(A.visited_sum) as visited_sum, hyperUniqueAggregator(A.unique_hosts) as unique_hosts;
describe C;

D = foreach C generate group, visited_sum, hyperUniquePostAgg(unique_hosts);
describe D;
dump D;
