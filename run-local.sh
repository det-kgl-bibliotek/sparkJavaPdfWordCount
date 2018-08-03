#!/usr/bin/env bash
set -e
set -x

#mvn package

# Use -DSpark.master=local[K]	to run locally with K cores

#Submit the job
mvn compile exec:java \
    -Dexec.classpathScope="compile" \
    -Dexec.cleanupDaemonThreads="false" \
    -Dexec.mainClass="dk.kb.JavaWordCount" \
    -Dspark.master=local \
    -Dexec.args="$HOME/Downloads/*.pdf"
