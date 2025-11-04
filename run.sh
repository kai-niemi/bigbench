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
profiles="default"

if [ ! -f "$jarfile" ]; then
    ./mvnw clean install
    ln -sf target/${jarfile} ${jarfile}
fi

java -jar ${jarfile} --profiles ${profiles} $*