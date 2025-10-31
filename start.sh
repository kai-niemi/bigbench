#!/bin/bash

if [ ! "$(command -v curl 2> /dev/null)" ]; then
	echo -e "[ FAIL ] Curl is not installed"
	exit 1
fi

if [ "$(whoami)" == "root" ]; then
    echo -e "[ FAIL ] Do NOT run as root!"
    exit 1
fi

jarfile=target/bigbench.jar
pidfile=work/bigbench.pid
profiles="default"

if [ -f ${pidfile} ]; then
   echo "Existing PID file ${pidfile} found - is it running?"
   exit 1
fi

mkdir -p work

nohup java -jar ${jarfile} --profiles ${profiles} --noshell $* > work/bigbench-stdout.log 2>&1 & echo $! > ${pidfile}

echo "Started service, check http://localhost:9090"