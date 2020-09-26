#!/bin/bash

installation_dir=/usr/local/apache-cassandra-3.11.8
if [[ -d $installation_dir ]]; then
	echo "Cassandra has already been installed"
	exit 1
fi

echo "Cassandra will be installed in $installation_dir"
sudo apt update
sudo apt -y install openjdk-8-jre-headless
sudo wget https://apache-mirror.rbc.ru/pub/apache/cassandra/3.11.8/apache-cassandra-3.11.8-bin.tar.gz

sudo tar xzf apache-cassandra-3.11.8-bin.tar.gz -C /usr/local/
cd $installation_dir

# $1 — название файла
# $2 — номер заменяемой строки
# $3 — заменяющая строка
replace_in_file() {
	sed_argument="'$2s/.*/$3'"
	temp_file=$1.temp

	sudo touch $temp_file
	sudo chown -R ubuntu:ubuntu $temp_file
	sudo sed "$2c\
$3" $1 > $temp_file

	sudo mv $temp_file $1
}

conf_file=conf/cassandra.yaml
backup_file=$conf_file.backup
sudo cp $conf_file $backup_file

replace_in_file $backup_file 612 "listen_address:"
replace_in_file $backup_file 675 "start_rpc: true"
replace_in_file $backup_file 689 "rpc_address:"

sudo mv $backup_file $conf_file

sudo chown -R ubuntu:ubuntu $installation_dir

sudo nano +425 $conf_file
