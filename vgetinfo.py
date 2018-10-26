import argparse                                 # for command line parsing
import vertica_db_client as hp_vertica_client   # for python client
import yaml                                     # for reading config file
from tabulate import tabulate                   # for pretty printing
import sqlparse                                 # for sql pretty formatting

def exec_txid(): ###show info for given txid / stid
  #(1) show summary statement from txid/stid 
  print "------------------------------------------------------ SUMMARY  --------------------------------------------------------"
  if args.date:
      datefilter = " and date(RI.\"time\") = '{0}' and date(RC.\"time\") = '{0}' ".format(args.date)
  else:
      datefilter = ""

  sql = "SELECT ri.request," \
        " datediff('ms',ri.time,rc.time) AS elapsed_ms," \
        " ri.request_type," \
        " ri.\"time\" AS start_timestamp," \
        " rc.\"time\" AS stop_timestamp," \
        " ri.user_name," \
        " ri.node_name," \
        " ri.session_id," \
        " ri.request_id," \
        " ri.is_retry," \
        " rc.success," \
        " rc.processed_row_count" \
        " FROM {0}requests_issued ri LEFT OUTER JOIN {0}requests_completed rc USING(session_id, request_id)" \
        " WHERE RI.transaction_id = {1} " \
        " {3} and RI.statement_id = {2} order by start_timestamp ".format(dcschema,args.txid,args.stid,datefilter)

  if args.debug: print sql
  cur = db.cursor()
  cur.execute(sql)
  rows = cur.fetchall()
  h = [desc[0]  for desc in  cur.description ]
  newlist = []
  for row in rows:
    (request,elapsed,request_type,start,stop,user_name,node_name,session_id,request_id,is_retry,success,processed_row_count) = row
    print sqlparse.format(row[0], reindent=True, keyword_case='upper')
    newlist.append(row[1:])
    print tabulate(newlist, headers = h[1:])    
  cur.close()

  #(2) show dc_errors
  print "---------------------------DC_ERRORS -----------------------------------------------------------------------------------"
  sql = "SELECT  time, message  FROM {0}_errors WHERE transaction_id = {1} and statement_id = {2} order by time; ".format("dc",args.txid,args.stid)
  prettyprint(sql) 

  #(3) show dc_resource_acquisitions 
  print "---------DC_RESOURCE_ACQUISITIONS---------"
  sql = """select       pool_name, 
 			 request_type,   
 			 RESULT,
 			 succeeded,
 			 min(time) AS "time",
 			 (avg(memory_kb)/1024/1024)::numeric(14,2)  AS avg_GB, 
 			 (max(memory_kb)/1024/1024)::numeric(14,2)  AS max_GB, 
 			 max(datediff('second', time,start_time)) AS waittime, 
 			 count(*) 
 			 FROM  {0}resource_acquisitions WHERE transaction_id = {1} AND statement_id = {2} GROUP BY 1,2,3,4 ORDER BY 5""".format(dcschema,args.txid,args.stid)
  prettyprint(sql)

  #3(b) memory additional acquired shown 
  sql ="select request_type, count(*) , (min(memory_kb)/1024/1024)::numeric(14,2) as min_gb, " \
       "(max(memory_kb)/1024/1024)::numeric(14,2) as max_gb , count(*) " \
       "FROM {0}resource_acquisitions WHERE transaction_id = {1}  AND statement_id =  {2} group by 1".format(dcschema,args.txid,args.stid)
  prettyprint(sql) 

  #(4) show explain plans
  print "---------- DC_EXPLAIN_PLANS -----------"
  sql = "SELECT path_id, path_line_index,path_line FROM dc_explain_plans  WHERE transaction_id = {0} and statement_id = {1} order by 1,2 ".format(args.txid,args.stid)
  prettyprint(sql) 

  #(5) show execution events / plan phases 
  print "---------- DC_EXECUTION_EVENTS ---------"
  sql = " select operator_name, event_type,event_description,event_details, suggested_action,  count(*), min(time), max(time)  from dc_execution_engine_events where transaction_id = {0}  and statement_id = {1} " \
        "group by 1,2,3,4,5 order by 6 desc; ".format(args.txid, args.stid)
  prettyprint(sql)

  #5(b), plan phases
  print "-----------DC_EXECUTION_STEPS------------"
  sql  = "select time, completion_time - time as elapsed, execution_step from dc_query_executions where transaction_id = {0} and statement_id = {1} " \
         "ORDER by 2 desc LIMIT 10".format(args.txid, args.stid)
  prettyprint(sql)

  print "-----------dc.eep(if exists)---------------"
  #5(c), dc_execution_engine_events
  sql = "select  counter_name, operator_name , path_id, (sum(counter_value)/1024/1024/1024)::numeric(14,2) as GB from {0}.eep " \
        "where regexp_like(node_name ,'_node0001') " \
        "and transaction_id = {1} and statement_id = {2}" \
        "and regexp_like(counter_name,'memory') group by 1,2,3 having (sum(counter_value)/1024/1024/1024) > 0.5 " \
        "order by 4 desc limit 10 ".format("dc", args.txid, args.stid)

  prettyprint(sql)

  #(6) show projections used 
  print "-----Projections_used + Projections ---------"
  sql = """select DISTINCT 
            			  P.projection_schema ||'.'||P.anchor_Table_name AS tablename
            			, P.projection_name, owner_name, created_epoch, has_statistics, is_segmented, is_super_projection  FROM
	(select * from {0}projections_used WHERE transaction_id = {1} and statement_id ={2}) PU 
	inner join projections P ON P.projection_id = PU.projection_oid ORDER BY is_segmented  """.format(dcschema,args.txid,args.stid)
  prettyprint(sql)

 #projection DDL
  sql = """select distinct P.projection_schema||'.'||anchor_table_name  FROM
        (select * from {0}projections_used WHERE transaction_id = {1} and statement_id ={2} ) PU 
        inner join projections P ON P.projection_id = PU.projection_oid; """.format(dcschema,args.txid,args.stid)
  if args.debug: print sql

  cur = db.cursor()
  cur.execute(sql)
  #h = [desc[0]  for desc in  cur.description ]
  if cur.rowcount > 0:
      for row in cur.fetchall():
        print "Table: " + row[0]
        sql = "select export_objects('', '"+row[0]+"');"
        prettyprint(sql)
  cur.close()
 


##### execute SQL command ####################################
def exec_sql():
  #sql="""select to_char(start_timestamp,'Mon-DD HH24:MI:SS TZ')  as ts,
#		(request_duration_ms/1000/60)::numeric(14,2) as minutes , 
#		substr(request,1,75) as short_sql, 
#		processed_row_count as rows , 
#		success , 
#		(reserved_extra_memory/1024/1024)::int extra_mem,
#		transaction_id, statement_id  from dc.query_summary_hist where 1=1 """
  sql = """SELECT
        datediff('ms',ri.time,rc.time) AS elapsed_ms,
        ri.request_type,
        ri."time" AS start_timestamp,
        rc."time" AS stop_timestamp, 
        ri.user_name,
        ri.node_name,
        ri.session_id,
        ri.transaction_id, 
	ri.statement_id,
        ri.request_id,
        ri.is_retry,
        rc.success,
        rc.processed_row_count,
	RA.mem_gb,
        substr(ri.request,1,75) as short_sql
        FROM {0}requests_issued ri INNER  JOIN {0}requests_completed rc USING (session_id, request_id) 
        left outer join (select transaction_id, statement_id, (max(memory_kb)/1024/1024)::numeric(14,2) mem_gb FROM {0}.resource_acquisitions group by 1,2 ) RA
		on RI.transaction_id = RA.transaction_id and RI.statement_id = RA.statement_id
        WHERE true """.format(dcschema)

 
  if args.sql:
	   sql += """ and regexp_like(request,'""" + args.sql  + """') """
  if args.username:
		sql += " and RI.user_name = '" + args.username  + "' and RC.user_name ='"+ args.username  + "'"
  sql+= " order by start_timestamp  DESC LIMIT 250;"

  prettyprint(sql)
#########################


#### helper function 
def prettyprint(sql):
 try : 
  cur=db.cursor()
  print sql
  cur.execute(sql)
  if cur.rowcount > 0 :
        rows = cur.fetchall()
        h = [desc[0]  for desc in  cur.description ]
  	print tabulate(rows, headers = h)
  else: 
	print "0 rows returned"
 except hp_vertica_client.NotSupportedError:
    print "NotSupportedError" 
 except hp_vertica_client.ProgrammingError:
    print "Error"
 cur.close()

parser = argparse.ArgumentParser(description='Extract troubleshooting info for given query',formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--sql', help='regular expression for queries')
parser.add_argument('--username', help='regular expression for queries')
parser.add_argument('--sessionid',  help='session_id ')
parser.add_argument('--stid',  help='statement_id')
parser.add_argument('--txid',  help='transaction_id ')
parser.add_argument('--requestid',  help='request_id ')
parser.add_argument('--config',  help='yaml config file section ')
parser.add_argument('--date',  help='date to filter on')
parser.add_argument('--debug', action='store_true' , help='debug' )

args = parser.parse_args()

with open('vgetinfo.yaml', 'r') as f:
    config= yaml.load(f)

host=config["root"][args.config]["host"]
username=config["root"][args.config]["username"]
password=config["root"][args.config]["password"]
db=config["root"][args.config]["db"]
port=config["root"][args.config]["port"]
dcschema=config["root"][args.config]["dcschema"]

db = hp_vertica_client.connect("host={0} database={1} port={2} user={3} password={4}".format(host, db, port, username, password)  )
cur = db.cursor()
cur.execute("set session timezone ='America/New_York';")
print "Times are in EST"


if args.txid:
  exec_txid()
else:
  exec_sql()

db.close()
#python27  vgetinfo.py --config gsn --sql "select device_platform, DATE\(eve.event_at AT TIME ZONE " --config gsn  --username gkrishnan
#python27  vgetinfo.py --config gsn  --txid 103582791479831049 --stid 7
#python27  vgetinfo.py --config openxxv --txid 225179981411037516 --stid 5 
#SELECT node_name,operator_name, path_id, (counter_value/1024/1024/1024)::numeric(14,2)      FROM vertica_history.eep1  where counter_name ='peak memory requested (bytes)' ORDER BY counter_value DESC LIMIT 10;
#SELECT counter_name, node_name,operator_name, path_id, (counter_value/1000/1000)::numeric(14,2) as secs      FROM vertica_history.eep1  where regexp_like(counter_name ,'\(us\)') ORDER BY counter_value DESC LIMIT 25;
