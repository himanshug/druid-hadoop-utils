-- REGISTER druid-pig-0.0.1-ALPHA.jar;

define hyperUniqueAggregator
define hyperUniquePostAgg com.yahoo.druid.pig.udfs.DoubleFinalizingComplexMetricAgg('{"size": 16384,"type": "sketchCount","name": "","fieldName": ""}', 'setSketch');

set druid.overlord.hostport localhost:${port}

A = load 'testDatasource' using com.yahoo.druid.pig.DruidStorage('/home/himanshu/work/druid-hadoop-utils/druid-pig/src/test/resources/sample-schema.json','1970-01-01T00:00:00.000/3000-01-01T00:00:00.000');
describe A;

B = group A by (druid_timestamp);
describe B;

C = foreach B generate group, SUM(A.visited_sum), hyperUniquePostAgg(A.unique_hosts);
describe C;

D = foreach C generate group, visited_sum, hyperUniquePostAgg(*);

describe D;
dump D;
