#!/bin/sh
#
# env variable DATABASE contains database name
# env variable LOGDIR contains directory for the log files
# usage: monitor_database.sh [sleeptime [repetitions]]

LOGDIR=${LOGDIR:-.}
DATABASE=${DATABASE:-test}
SLEEP=${1:-0}
REPETITIONS=${2:-1}

db2 connect to  $DATABASE

i=0
while [ $i -lt $REPETITIONS ]
do
   date
   db2 "select member, total_act_time, total_cpu_time, direct_reads, direct_read_time, direct_writes, direct_write_time, pool_read_time, pool_write_time from table(mon_get_database(-2))"
   i=`expr $i + 1`
   if [ $i -lt $REPETITIONS ]
   then 
     sleep $SLEEP
   fi
done > $LOGDIR/db-${DATABASE}-`date +%Y%m%d_%H%M%S`.log

db2 terminate