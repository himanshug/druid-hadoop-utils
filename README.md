## About
This is a collection of utilities to read druid segments stored on hdfs from hadoop. It contains a hadoop input format, pig loader and pig udf for druid complex metrics.
This code is a prototype really and in very early stages, so some of the details might change. That said, I did test it to be working and will update as and when necessary. If you have any questions, please post them to druid community user groups.

## Quick Start
1. Get the code: `git clone https://github.com/himanshug/druid-hadoop-utils.git`
1. Build: `mvn clean package`
1. `mvn dependency:copy-dependencies` to download required dependencies
1. create javadocs : `mvn javadoc:javadoc` . docs will be in submodule/target/site/apidocs/
1. For help on druid hadoop Input Format, see javadoc of DruidInputFormat.
1. For druid pig loader, see javadoc of DruidStorage
