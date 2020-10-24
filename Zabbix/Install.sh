#!/bin/sh

postgresqlHost=localhost

# Download and install
sudo wget https://repo.zabbix.com/zabbix/5.0/ubuntu/pool/main/z/zabbix-release/zabbix-release_5.0-1+focal_all.deb
sudo dpkg -i zabbix-release_5.0-1+focal_all.deb
sudo apt update
sudo apt install -y postgresql-client-12 zabbix-server-pgsql zabbix-frontend-php php7.4-pgsql zabbix-nginx-conf zabbix-agent

# Initialize database
zcat /usr/share/doc/zabbix-server-pgsql*/create.sql.gz | sudo psql -h $postgresqlHost -d zabbix -U zabbix

# Configure Zabbix

echo "In /etc/zabbix/zabbix_server.conf:"
echo "	Set LogFileSize, DBHost, DBPassword"
sudo nano /etc/zabbix/zabbix_server.conf

echo "In /etc/zabbix/nginx.conf:"
echo "	Uncomment lines listen and server_name"
echo "	Assign public IP or DNS name to server_name"
sudo nano +2 /etc/zabbix/nginx.conf

echo "In /etc/zabbix/php-fpm.conf:"
echo "	Uncomment and set php_value[date.timezone]"
sudo nano +24 /etc/zabbix/php-fpm.conf

sudo systemctl restart zabbix-server zabbix-agent nginx php7.4-fpm
sudo systemctl enable zabbix-server zabbix-agent nginx php7.4-fpm
