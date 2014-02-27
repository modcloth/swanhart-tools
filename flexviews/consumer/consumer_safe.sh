#!/bin/bash

cd $HOME/swanhart-tools/flexviews/consumer
echo `pwd`

php run_consumer.php --pid=flexcdc.pid $*  2>&1 >> $HOME/flex_cdc_logs/flex_cdc_log.log 
if [ $? -eq 0 ]; then
    echo "Started run_consumer.php" | mailx -s "Started FlexCDC" dwh_alerts@modcloth.com
    exit 0
else
    echo "run_consumer.php is not running" | mailx -s "ERROR! FlexCDC is not running" dwh_alerts@modcloth.com
exit 1
fi

