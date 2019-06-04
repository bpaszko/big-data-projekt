#!/bin/sh

hive -e 'set hive.cli.print.header=true; select * from smog_prediction where dt > current_date;' \
| sed 's/[\t]/,/g' > /tmp/smog_pred.csv
gsutil cp /tmp/smog_pred.csv gs://smog_pred_bucket/
rm /tmp/smog_pred.csv