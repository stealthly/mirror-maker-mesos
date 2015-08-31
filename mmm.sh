#!/bin/sh

JAR_FILE='mmm-0.1-SNAPSHOT.jar'

java -Dlog4j.configuration=config/mmm-log4j.properties -jar $JAR_FILE "$@"