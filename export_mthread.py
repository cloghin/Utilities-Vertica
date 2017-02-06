# System modules
import argparse
from Queue import Queue
from threading import Thread
from subprocess import call,PIPE,Popen
import time

## Need to upgrade the script to use a message queue across multiple hosts 
## so that we can add workers on new hosts or kill them. can use rabbitMQ on test cluster 
## and dispatch work on other hosts to get work. May need to add at the beginning of the queue or the end 
##  of the queue


# Set up some global variables
num_fetch_threads = 4
field_delim=""
null_char  =""
record_terminator="\n"
 
export_queue = Queue()

def export_table(i, q):
    while True:
        item = q.get() #extract queue element by worker , contains a list of 2 entries ( schema.tablename, partition) 
	(schema, table)  = item[0].split('.') # split into list of (schema,table_name)
	partition = item[1]

	if (partition == None or table == None):
		print "Received empty partiton %s for table %s , finished work early" % (partition , table)
		q.task_done()
	else: 
		s3= s3location+ "/" +table +"/"+partition +"/" + item[0] + "_" + partition +  ".bz2"
		sql = "select * from "+ item[0] + "  where date(gaia_create_dt) = '" + partition + "' ;"
	 
		start_time = time.time() #measure elapsed time
		if (not args.drymode):
		
			p1 = Popen(vsql_args + [sql] ,stdout=PIPE)	
			p2 = Popen(["bzip2","--fast"], stdin=p1.stdout, stdout=PIPE)
			p3 = Popen(["aws","s3","cp","-",s3],stdin=p2.stdout )

			p1.stdout.close()
			p2.stdout.close()

			print p3.communicate()[1] #show any errors 
		else:
			print "Export to %s " % s3

		print "Elapsed time:  %d(sec) for %s_%s" %  ( time.time() - start_time , table, partition  ) 
        	q.task_done()
#### finished export worker function 

parser = argparse.ArgumentParser(description='Multithreaded table export process,python export_mthread.py --type event  --password `tail -1 pass_file | cut -d' ' -f3` ')
parser.add_argument('--type',required=True,help='export type to perform:  profile|event')
parser.add_argument('--password',required=True, help='vertica dbadmin\'s password')
parser.add_argument('--tbname', help='export this table_name only')
parser.add_argument('--drymode', help="run in dry mode, do not export anything", action="store_true")


args = parser.parse_args()


# Set up some threads to start exporting
for i in range(num_fetch_threads):
    worker = Thread(target=export_table, args=(i, export_queue,))
    worker.setDaemon(True)
    worker.start()


if (args.type == "profile"):
    w_clause="where (regexp_like(anchor_table_name,'^profile_') is true OR regexp_like(anchor_table_name,'^npc_Npc') is true ) "
    s3location="s3://wba-monolith-firebird/prodprofile/psv/profile/current"
elif (args.type == "event"):
    w_clause="""where (NOT regexp_like(anchor_table_name,'^profile_') is true 
		AND NOT regexp_like(anchor_table_name,'^npc_Npc') is true 
		AND NOT regexp_like(anchor_table_name,'^temp_profile') is true) """
    s3location="s3://wba-monolith-firebird/prodevent/psv/event/current"
else:
    print "Unknown type provided. Run --help to see command line options. Exiting..."
    exit(0)
     
if (args.tbname != None ):
	w_clause += "and anchor_Table_name='" + args.tbname +  "' " 


sql="""select projection_schema ||'.'||anchor_table_name
		from projection_storage """ + w_clause + """
		and projection_schema ='firebird_production_history' 
		group by 1 having sum(used_bytes) > 0  
		order by sum(used_bytes) ASC; """

vsql_args = ["vsql" ,"-w",args.password,"-F",field_delim,"-P","null="+null_char,"-XtAc" ] 


def create_input_file():
	p=Popen(vsql_args + [sql] ,stdout=PIPE)
	(out,err) = p.communicate()
	f = open(args.type + "_input","w")

	for table_name  in out.splitlines():
            print 'Queuing:', table_name
	    sql = "select date(gaia_create_dt) ,count(*) as rc from " + table_name + " group by 1 order by 1;"
            p=Popen(vsql_args + [sql] ,stdout=PIPE)
            (out,err) = p.communicate()
            for line in out.splitlines():
		(partition,rc ) = line.split(field_delim)
		if ( partition != None ) :
            		#export_queue.put([table_name, partition] )
			f.write(table_name +"\t"+ partition + "\t" + rc + "\n")
		else:
			print "Empty partition for %s" % partition

	f.close()

f = open(args.type + "_input","r")
for line in f:
	(table_name,partition,rc) = line.split('\t')
	print "%s %s" % (table_name,partition)
	export_queue.put([table_name, partition] )

f.close()
        
# Now wait for the queue to be empty, indicating that we have
# processed all of the exports
print '*** Main thread waiting'
export_queue.join()
print '*** Done exporting'

#Invokation example 
#python export_mthread.py --type profile --password `tail -1 SCRIPTS/pass_file | cut -d' ' -f3` --drymode
