#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

#JAVA_OPTS="-Djava.rmi.server.codebase=file://$PATH_TO_CODE_BASE/lib/jars/rmi-params-client-1.0-SNAPSHOT.jar"

QUERY="1"
MAIN_CLASS="ar.edu.itba.pod.tpe2.client.Client"

java $JAVA_OPTS $* -Dquery=$QUERY -cp 'lib/jars/*'  $MAIN_CLASS
