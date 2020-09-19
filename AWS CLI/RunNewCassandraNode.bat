call aws ec2 run-instances --image-id ami-0dba2cb6798deb6d8 --count 1 --instance-type t2.large --associate-public-ip-address --key-name cassandra-node --security-group-ids sg-2f1d6913 --subnet-id subnet-c01771ce

pause
