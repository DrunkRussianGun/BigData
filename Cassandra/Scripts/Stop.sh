#!/bin/bash

installation_dir=/usr/local/apache-cassandra-3.11.8
if [[ ! -d $installation_dir ]]; then
	echo "Cassandra hasn't been installed"
	exit 1
fi

cd $installation_dir
pid_file=bin/pid.txt
kill `cat $pid_file`
