#!/bin/bash

if [ ! "$(command -v curl 2> /dev/null)" ]; then
	echo -e "[ FAIL ] Curl is not installed"
	exit 1
fi

if [ "$(whoami)" == "root" ]; then
    echo -e "[ FAIL ] Do NOT run as root!"
    exit 1
fi

jarfile=bigbench.jar
pidfile=work/bigbench.pid
profiles="default"

if [ ! -f "$jarfile" ]; then
    ./mvnw clean install
    ln -sf target/${jarfile} ${jarfile}
fi

if [ -f ${pidfile} ]; then
   echo "Existing PID file ${pidfile} found - is it running?"
   exit 1
fi

mkdir -p .log
mkdir -p work

nohup java -jar ${jarfile} --profiles ${profiles} --noshell $* > .log/bigbench-stdout.log 2>&1 & echo $! > ${pidfile}

sleep 3
cat .log/bigbench-stdout.log

open http://localhost:9090

echo "Started service, check .log dir or http://localhost:9090"
