#US gov logfile parser

This is a stupidly simple project, I hacked this together in the late evening (therefore it is java, the only language I can write when falling asleep).

## Installation instructions

Fire up an elasticsearch instance at localhost and add an index and a mapping

```
curl -X PUT localhost:9200/logfile
curl -X PUT localhost:9200/logfile/log/_mapping -d '
{
    "log" : {
        "properties" : {
            "geohash" : {
                "type" : "geo_point",
                "geohash": true
            }
        }
    }
}'
```

Clone this repo and fire up the indexer

```
git clone https://github.com/spinscale/usgov-logfile-parser.git
cd usgov-logfile-parser
mvn package
java -jar target/usgov-logfile-parser-1.0-SNAPSHOT.jar
```

From then on the service access the stream and inserts data into elasticsearch (by default localhost in index). Only the timestamp and the location is extraced. The location is converted to a geo hash before it is indexed. Data is inserted roughly every 10 seconds or whenever 100 requests are reached before that. Some cheap stats are outputted after every indexation.