#!/usr/bin/env bash

APPNAME="trendingtopics"
CLASSPATH="`ls -x ../../voltdb/voltdb-*.jar | tr '[:space:]' ':'``ls -x ../../lib/*.jar | tr '[:space:]' ':'`"
VOLTDB="../../bin/voltdb"
VOLTCOMPILER="../../bin/voltcompiler"
LOG4J="`pwd`/../../voltdb/log4j.xml"
LICENSE="../../voltdb/license.xml"
LEADER="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/trendingtopics/*.java \
        src/trendingtopics/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
	echo "Done"
    $VOLTCOMPILER obj project.xml $APPNAME.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
	echo "NOt AGAIN"
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE leader $LEADER
}


# run the client that drives the example
function client() {
    trendingtopics
}

function trendingtopics(){
    srccompile
    java -classpath obj:$CLASSPATH:obj trendingtopics.trendingtopics
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# = 1 ]; then $1; else server; fi
