#!/bin/sh
# 

ss_base=`echo $0 | awk -F/ '{for (i=1;i<NF;i++){printf $i "/"}}' | sed 's/\/$//'`
date_stamp=`date +%y%m%d.%H%M%S`
cmd="${ss_base}/selective_ldap_sync.pl $* -c ${ss_base}/selective_ldap_sync_sdp.cf"
log=${ss_base}/logs/selective_ldap_sync.out.${date_stamp}

if [ ! -d ${ss_base} ]; then
    echo "create or make writable ${ss_base}/logs!"
    exit
fi
#echo $cmd
$cmd 2>&1 | tee $log

# remove the log of it's empty, print it's location if not.
if [ ! -s $log ]; then
    rm $log
else 
    echo; echo "** output logged to ${log}"
fi
