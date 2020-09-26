@echo off
set nodes_count=1
if not [%1]==[] set nodes_count=%1

@echo on
call aws ec2 run-instances --no-paginate --image-id ami-0dba2cb6798deb6d8 --count %nodes_count% --instance-type t2.large --associate-public-ip-address --key-name cassandra-node --security-group-ids sg-2f1d6913 --subnet-id subnet-c01771ce --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=30}" --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=Cassandra Node}]" "ResourceType=volume,Tags=[{Key=Name,Value=Cassandra Node}]"

pause
