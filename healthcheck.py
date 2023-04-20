import argparse
import hp_vertica_client as vertica
import yaml
from tabulate import tabulate                   # for pretty printing

#### helper function
def prettyprint(sql):
 if args.debug:
     print sql
 try :
  cur=db.cursor()
  cur.execute(sql)
  if cur.rowcount > 0 :
    rows = cur.fetchall()
    h = [desc[0]  for desc in  cur.description ]
    print tabulate(rows, headers = h)
  else:
    print "0 rows returned"
 except vertica.Error as e :
    print e
 finally:
    cur.close()

def getLongQueries():

    sql = " select  f.time - s.time as duration," \
          " s.time as starttime," \
          " s.user_name," \
          " substr(regexp_replace(s.request, E'\\n|\\r'::varchar(5), ' '::varchar(1), 1, 0, ''::varchar), 1, 150) as short_sql ," \
          " s.session_id," \
          " s.request_id" \
          " from dc.requests_issued s  inner join dc.requests_completed f on s.session_id = f.session_id and s.request_id = f.request_id" \
          " where f.time >= current_timestamp - '7 day'::interval and s.time >= current_timestamp - '7 day'::interval order by duration DESC LIMIT 25;"

    prettyprint(sql)

def analyzeByLabel(label):
    print "Top duration - detail"
    sql = "select duration_sec, user_name,starttime, session_id, transaction_id, statement_id , " \
          " substr(regexp_replace(request, E'\\n|\\r', ' ',1, 0,''), 1, 150) as sql  from dc.rirc where label = '{l}' order by duration_sec desc limit 25;".format(l=label)
    prettyprint(sql)

    print "Top duration - aggregate"
    sql="""select substr(request,1,150) , 
            success,
            avg(duration_sec)::numeric(14,2),
            max(duration_sec),
            count(*) from dc.rirc where label = '{l}'
            group by 1,2;""".format(l=label)
    prettyprint(sql)

    print "Top memory - detail"
    sql ="""select RA.transaction_id, RA.statement_id, RI.ssql, min(RA."time") as starttime,
            max(memory_kb)/(1024^2) as mem_gb from dc.resource_acquisitions RA 
            inner join ( select transaction_id,statement_id , substr(request,1,25) as ssql from dc.requests_issued 
                                where label='{l}' and time > CURRENT_TIMESTAMP - {d} ) RI using (transaction_id,statement_id)
            where RA."time"  > CURRENT_TIMESTAMP - {d} group by 1,2,3 order by 5 desc LIMIT 100; """.format(d=args.days,l=label)
    prettyprint(sql)

    print "Top memory - agregate"
    sql ="""select min(mem_gb) min_gb, avg(mem_gb) avg_gb, max(mem_gb) max_gb, count(*)  FROM (select RA.transaction_id, RA.statement_id, 
            max(memory_kb)/(1024^2) as mem_gb from dc.resource_acquisitions RA 
            inner join ( select transaction_id,statement_id from dc.requests_issued 
                                where label='{l}' and time > CURRENT_TIMESTAMP - {d} ) RI using (transaction_id,statement_id)
            where RA."time"  > CURRENT_TIMESTAMP - {d} group by 1,2) X  """.format(d=args.days,l=label)
    prettyprint(sql)


def analyzeByString(pattern):
    print "Top duration - detail"
    sql = "select duration_sec, user_name,starttime, session_id, transaction_id, statement_id , " \
          " substr(regexp_replace(request, E'\\n|\\r', ' ',1, 0,''), 1, 150) as sql  from dc.rirc where regexp_like(request,'{p}') order by duration_sec desc limit 25;".format(p=pattern)
    prettyprint(sql)

    print "Top duration - aggregate"
    sql="""select substr(request,1,150) , 
            success,
            avg(duration_sec)::numeric(14,2),
            max(duration_sec),
            count(*) from dc.rirc where regexp_like(request,'{p}')
            group by 1,2;""".format(p=pattern)
    prettyprint(sql)

    print "Top memory - detail"
    sql ="""select RA.transaction_id, RA.statement_id, RI.ssql, min(RA."time") as starttime,
            max(memory_kb)/(1024^2) as mem_gb from dc.resource_acquisitions RA 
            inner join ( select transaction_id,statement_id , substr(request,1,25) as ssql from dc.requests_issued 
                                where regexp_like(request,'{p}') and time > CURRENT_TIMESTAMP - {d} ) RI using (transaction_id,statement_id)
            where RA."time"  > CURRENT_TIMESTAMP - {d} group by 1,2,3 order by 5 desc LIMIT 100; """.format(d=args.days,p=pattern)
    prettyprint(sql)

    print "Top memory - agregate"
    sql ="""select min(mem_gb) min_gb, avg(mem_gb) avg_gb, max(mem_gb) max_gb, count(*)  FROM (select RA.transaction_id, RA.statement_id, 
            max(memory_kb)/(1024^2) as mem_gb from dc.resource_acquisitions RA 
            inner join ( select transaction_id,statement_id from dc.requests_issued 
                                where regexp_like(request,'{p}') and time > CURRENT_TIMESTAMP - {d} ) RI using (transaction_id,statement_id)
            where RA."time"  > CURRENT_TIMESTAMP - {d} group by 1,2) X  """.format(d=args.days,p=pattern)
    prettyprint(sql)

def getTopMemoryQueries():
    label = ""
    if args.label:
        label = " AND I.label ='{0}' ".format(args.label)

    sql= " select  SUB.pool_name,SUB.mem_gb, to_char(SUB.start_time,'YYYYMMDD-HH:MI:SS') as time, I.label, I.transaction_id, I.statement_id, " \
         " substr(regexp_replace(I.request, E'\\n|\\r'::varchar(5), ' '::varchar(1), 1, 0, ''::varchar), 1, 150) as short_sql" \
         " FROM (select " \
         " transaction_id," \
         " statement_id," \
         " pool_name," \
         " result, " \
         " max(memory_kb)/(1024^2) as mem_gb," \
         " min(time) start_time " \
         " \nFROM dc.resource_acquisitions A " \
         " \n\twhere A.time >= current_timestamp - '{d} day'::interval " \
         " \n\tGROUP BY 1,2,3,4 ORDER BY 5 DESC LIMIT 300) SUB" \
         " LEFT JOIN dc.requests_issued I USING ( transaction_id ,statement_id )" \
         " WHERE I.time >= current_timestamp - '{d} day'::interval" \
         "{l}" \
         " \nORDER BY SUB.mem_gb DESC;".format(d=args.days,l=label)

    prettyprint(sql)

parser = argparse.ArgumentParser(description='Cluster performance - Txt Version',formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--email', help='email address to send report to')
parser.add_argument('--config',default='gsn', help='company')
parser.add_argument('--debug', help='produce verbose info', action='store_true')
parser.add_argument('--days', default=3, help='days backwards')
parser.add_argument('--label',  help='check on specific query label only')
parser.add_argument('--pattern',  help='check on specific query string pattern -regexp -  only')

args = parser.parse_args()

with open('vgetinfo.yaml', 'r') as f:
    config= yaml.load(f)

host=config["root"][args.config]["host"]
username=config["root"][args.config]["username"]
password=config["root"][args.config]["password"]
db=config["root"][args.config]["db"]
port=config["root"][args.config]["port"]
dcschema=config["root"][args.config]["dcschema"]

db = vertica.connect("host={0} database={1} port={2} user={3} password={4}".format(host, db, port, username, password))

cur = db.cursor()
cur.execute("set session timezone ='America/New_York';")
print "Times are in EST"

#getLongQueries()
getTopMemoryQueries()


if args.label:
    analyzeByLabel(args.label)
    getTopMemoryQueries()

if args.pattern:
    analyzeByString(args.pattern)
    #getTopMemoryQueries()

db.close()
