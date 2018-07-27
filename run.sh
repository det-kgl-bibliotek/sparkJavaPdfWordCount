#!/usr/bin/env bash
set -e
set -x

mvn clean package

scp target/spark-pdf-example-1.0-SNAPSHOT.jar $USER@kac-proj-000:.

ssh $USER@kac-proj-000 <<EOF
export SPARK_MAJOR_VERSION=2 #Or set this in your .bashrc
spark-submit \
    --master=yarn \
    --deploy-mode client \
    --class dk.kb.JavaWordCount \
    spark-pdf-example-1.0-SNAPSHOT.jar \
    /user/\$USER/pdfs
EOF