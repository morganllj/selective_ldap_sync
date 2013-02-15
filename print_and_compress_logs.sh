#!/bin/sh
# 
# this should not run at the same minute as selective_sync.pl as it will capture the active log file

base=`echo $0 | awk -F/ '{for (i=1;i<NF;i++){printf $i "/"}}' | sed 's/\/$//'`
date_stamp=`date +%y%m%d.%H%M%S`
logdir=${base}/logs

if [ ! -d ${base} ]; then
    echo "create or make writable ${base}/logs!"
    exit
fi

files=`cd ${logdir}; ls |egrep -v 'gz$'|sort`

if [ x != "x${files}" ]; then
    echo "new files: $files"
    (cd ${logdir}; cat $files)
    echo; echo "archiving logs.."
    (cd ${logdir}; tar cvfz archived_${date_stamp}.tar.gz ${files} && rm $files)
else
    echo "no new files.."
fi
