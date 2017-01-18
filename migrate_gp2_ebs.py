from subprocess import call, PIPE,Popen
import thread

type="st1"
avail_zone="us-east-1d" 

db="...databasename..."
#instances = ['i-5f403ba5','i-5b403ba1','i-63403b99','i-60403b9a','i-55403baf' ]  # netherrealm
#instances = [ 'i-cadc4521','i-59db42b2','i-5fdb42b4','i-5cdb42b7','i-a3dc4548']  #  monolith
instances = [ 'i-cadc4521','i-59db42b2','i-5fdb42b4','i-5cdb42b7','i-a3dc4548' , 'i-5f403ba5','i-5b403ba1','i-63403b99','i-60403b9a','i-55403baf' ] #all 

def create_volumes():
  for host in instances:
 	print host
 	f = open("instances/" + host, "a+")
 	for x in range(1):
      		call(["aws" ,"ec2","create-volume","--size","1024","--availability-zone", avail_zone  ,"--volume-type", "st1",  "--query", 'VolumeId' ,"--output", "text"], stdout=f)
 	f.close()

def attach_volumes():
    for host in instances:
        print host
	letters  = ['a','b','c','d','e','f','g','h','i','j']
	for index,volume in enumerate(open("instances/" + host,'r')):
  		call(["aws","ec2", "attach-volume" , "--volume-id" , volume  , "--instance", host ,"--device" , "xvdd" + letters[index] ])

def create_adm():
	for host in instances:
		#aws ec2 describe-instances  --instance-ids i-5cdb42b7  --query "Reservations[*].Instances[*].PrivateIpAddress"   --output=text
		p = Popen(["aws","ec2", "describe-instances" , "--instance-ids", host, "--query", "Reservations[*].Instances[*].PrivateIpAddress"  , "--output", "text"],stdout=PIPE)
		(out,err) = p.communicate()
		print out
		#call(["ssh","-o","StrictHostKeyChecking=no","-i","/home/dbadmin/.ssh/id_rsa_root","root@"+out,"mdadm -Cv /dev/md126 -l0 --chunk=1024 -n10 /dev/xvdd[abcdefghij]"])
		#call(["ssh","-o","StrictHostKeyChecking=no", "-i" ,"/home/dbadmin/.ssh/id_rsa_root", "root@"+out,"mkdir /vertica/data_st1; chown dbadmin:dbadmin /vertica/data_st1"])
		#call(["ssh", "-o", "StrictHostKeyChecking=no" , "-i" ,"/home/dbadmin/.ssh/id_rsa_root", "root@" + out ,  "ls -al   /vertica/data_st1"])

		#download_thread = threading.Thread(target=create_fs, args= host )
    		#download_thread.start()
		#thread.start_new_thread( create_fs, (out,) )

		#call(["ssh", "-o", "StrictHostKeyChecking=no" , "-i" ,"/home/dbadmin/.ssh/id_rsa_root", "root@" + out ,  "chown dbadmin:dbadmin /vertica/data_st1"])
		#call(["ssh", "-o", "StrictHostKeyChecking=no",  out ,  "cd  /vertica/data_st1; mkdir backups; mkdir tmp ; mkdir " + db  ])
		#call(["ssh", "-o", "StrictHostKeyChecking=no",  out ,  "mkdir /vertica/data_st1/" + db ])
		
		#print "SELECT add_location( '/vertica/data_st1/"+db + "/v_"+db+ "_node000X_data' , 'v_"+db+"_node000X', 'DATA,TEMP');"
		#print "SELECT retire_location('/vertica/data/"+db+"/v_"+db+"_node000X_data','v_"+db+"_node000X', true);"
		
		#call(["ssh", "-o", "StrictHostKeyChecking=no", "-i" ,"/home/dbadmin/.ssh/id_rsa_root", "root@" + out ,  " df -h " ])
		#call(["ssh", "-o", "StrictHostKeyChecking=no" , "-i" ,"/home/dbadmin/.ssh/id_rsa_root", "root@" + out ,  "mount -t ext4 /dev/md126 /vertica/data_st1"])

def create_fs(h):
	call(["ssh" , "-o", "StrictHostKeyChecking=no",  "-i" ,"/home/dbadmin/.ssh/id_rsa_root", "root@" + h ,  "mkfs -t ext4 /dev/md126"])


def cleanup_volumes():
   #call(["aws","ec2", "detach-volume" , "--volume-id" , volume  , "--instance", instanceid  ])
   #call(["aws","ec2", "delete-volume" , "--volume-id" , volume  ])
   #aws ec2 describe-instances  --instance-ids i-5cdb42b7  --query "Reservations[*].Instances[*].BlockDeviceMappings[${i}].Ebs.{I:VolumeId}" 
   for host in instances:
      f = open("instances/vol_" + host, "a+")
      call(["aws","ec2", "describe-instances" , "--instance-ids", host, "--query", "Reservations[*].Instances[*].BlockDeviceMappings[*]"  , "--output", "text"],stdout=f)
      f.close()

def print_msg():
	print "mdadm -Cv /dev/md126 -l0 --chunk=1024  -n10 /dev/xvdd[abcdefghij];"
	print "mkdir /vertica/data_st1;"
	print "mkfs -t ext4 /dev/md126"
	print "edit /etc/fstab , mount /dev/md126 /vertica/data_st1" 

def tag_volumes():
	for host in instances:
		
	

