#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR=$(dirname $(readlink -f $BASH_SOURCE[0]))

cd $SCRIPT_DIR

mvn clean package

scp target/spark-pdf-example-*.jar $USER@kac-proj-000:.

# -t is so that if we Ctrl-C this operation, the java process is not kept running on the remote server
# --deploy-mode client so that if the java process on the remote server dies, the entire job dies. Otherwise the Ctrl-C
# might not kill the job
ssh -t $USER@kac-proj-000 env SPARK_MAJOR_VERSION=2 spark-submit \
    --master=yarn \
    --deploy-mode client \
    --class dk.kb.JavaWordCount \
    spark-pdf-example-*.jar \
    /user/\$USER/pdfs
