#!/bin/bash

cd $HOME/swanhart-tools/flexviews/consumer
echo `pwd`
 
FAILURE_EMAIL_PREFIX=$(grep email_message_header consumer.ini|cut -d= -f2)
FAILURE_EMAIL_ADDRESS=$(grep failure_email_address consumer.ini|cut -d= -f2)
 
php run_consumer.php --pid=flexcdc.pid $*  2>&1 >> $HOME/flex_cdc_logs/run_consumer_php.log 

status=$?

if [ "$status" -eq 0 ]; then
    echo "Started run_consumer.php" | mailx -s "[$FAILURE_EMAIL_PREFIX] Started FlexCDC" $FAILURE_EMAIL_ADDRESS
    exit 0
else
  if [ "$status" -ne 8 ]; then
    echo "run_consumer.php is not running ($status)" | mailx -s "[$FAILURE_EMAIL_PREFIX] ERROR! FlexCDC is not running" $FAILURE_EMAIL_ADDRESS
    exit 1
  fi
fi

