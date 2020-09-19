call aws ec2 describe-instances --no-paginate --query "Reservations[*].Instances[*].{InstanceId:InstanceId,Name:Tags[?Key==`Name`]|[0].Value,PublicDnsName:PublicDnsName,State:State.Name}" --output text

pause
