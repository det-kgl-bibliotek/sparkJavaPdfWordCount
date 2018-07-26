#!/usr/bin/env bash
set -e
set -x

mvn package

#Get a version of spark
wget -N "https://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz"

#Extract
tar -xvzf spark-2.1.1-bin-hadoop2.7.tgz --keep-newer-files

#Start the local master
spark-2.1.1-bin-hadoop2.7/sbin/start-master.sh

#Submit the job
spark-2.1.1-bin-hadoop2.7/bin/spark-submit \
    --master local \
    --class dk.kb.JavaWordCount \
    target/spark-pdf-example-1.0-SNAPSHOT.jar \
    "$HOME/Downloads/*.pdf"

#Stop the master
spark-2.1.1-bin-hadoop2.7/sbin/stop-master.sh
