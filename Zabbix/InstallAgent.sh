#!/bin/sh

# Download and install
sudo wget https://repo.zabbix.com/zabbix/5.0/ubuntu/pool/main/z/zabbix-release/zabbix-release_5.0-1+focal_all.deb
sudo dpkg -i zabbix-release_5.0-1+focal_all.deb
sudo apt update
sudo apt install -y zabbix-agent

# Configure
echo "In /etc/zabbix/zabbix_agentd.conf:"
echo "	Set LogFileSize, Server"
sudo nano /etc/zabbix/zabbix_agentd.conf

sudo systemctl restart zabbix-agent
sudo systemctl enable zabbix-agent
