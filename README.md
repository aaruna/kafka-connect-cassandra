# Kafka Connector for Cassandra
A Kafka Connect Cassandra Source and Sink connector.

## Versions
The Kafka version is 0.9.0.1.

The Cassandra version defaults to 3.0.0, however this is configurable in the build with:

    sbt -Dcassandra.version=2.2.2
    
## Build

## Run
### Tests
Run unit tests in your IDE or by command line:

    sbt test
    sbt testOnly com.tuplejump.kafka.connector.SomeSpecName
        
Integration tests use the Cassandra plugin, and currently need to be run as all vs singular:

    sbt it:test
    
## Committers
- Shiti Saxena [@eraoferrors](https://twitter.com/eraoferrors)
- Helena Edelson [@helenaedelson](https://twitter.com/helenaedelson)

### Contributors
