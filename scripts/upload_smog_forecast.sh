#!/bin/sh

hive -e 'select * from smog_prediction where dt > current_date;' \
| sed 's/[[:space:]]\+/,/g' > /tmp/smog_pred.csv
gsutil cp /tmp/smog_pred.csv gs://smog_pred_bucket/
rm /tmp/smog_pred.csv

