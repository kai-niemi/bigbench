#!/bin/bash

if [ ! "$(command -v curl 2> /dev/null)" ]; then
	echo -e "[ FAIL ] Curl is not installed"
	exit 1
fi

if [ "$(whoami)" == "root" ]; then
    echo -e "[ FAIL ] Do NOT run as root!"
    exit 1
fi

pidfile=work/bigbench.pid

if [ ! -f ${pidfile} ]; then
   echo "No bigbench PID found - is it running?"
   exit 1
fi

PID=`cat ${pidfile}`
kill -TERM $PID
RETVAL=$?

echo "Stopped service (pid: $PID) code $RETVAL"

rm -f ${pidfile}
