import argparse,sys
import hp_vertica_client

import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
from cycler import cycler

import datetime
import numpy as np
import os

# Import the email modules we'll need
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart


plt.rc('axes', prop_cycle=(cycler('color', ['#e41a1c', '#377eb8', '#4daf4a', '#ff7f00', '#a65628', '#f781bf', '#999999', '#984ea3', '#ffff33','#7fff00',
                                            '#F0A3FF', '#0075DC', '#993F00', '#4C005C', '#191919']) ))

font = {'size': 8}
plt.rc('font', **font)

#memory usage 
def exec_memusage(message):
   #left axis : reservedmemory (GB) , borrowedmemory (GB) , right axis ( concurrency) , legend inside 
   #memory - full line, borrowed memory dash + point ,concurrency point
   global args
   pool_name_not_in ="('dbd','jvm','recovery','refresh')"
   #get maxconcurrency to plot in graphs
   cur=db.cursor()
   cur.execute("""select * from 
		(select pool_name, (declared_size_memory_kb_start_value/1024/1024)::integer as memsize,  
	        (limit_memory_kb_start_value/1024/1024)::integer as maxmemsize , 
		planned_concurrency_start_value ,limit_queries_start_value ,
		row_number() over(partition by pool_name order by time DESC ) RN
 		FROM dc_resource_pool_status_by_""" + args.grain + """ 
	 	where pool_name NOT IN """ + pool_name_not_in + """ and time > current_date - """ + str(args.days) + """ ) x
 		where x.rn = 1 order by 1 ASC;""")

   rows = cur.fetchall()

   dict ={}
   for row in rows:
      dict[str(row[0])] = str(row[1]) + "G/" + str(row[2]) + "G/" + str(row[3]) + "/" + str(row[4])
   cur.close()
   no_subplots = len(rows) #cannot add subplots dynamically  so we need to count them ahead of time 
   
   cur = db.cursor()
   cur.execute("set session timezone ='America/New_York';")


   cur = db.cursor()
   cur.execute(""" select       pool_name,
                                date_trunc('""" + args.grain +"""',time)::timestamp as hour, 
                                max(reserved_memory_kb_max_value/1024/1024)::integer - 
				CASE WHEN (max(reserved_memory_kb_max_value - declared_size_memory_kb_start_value)/1024/1024)::integer  > 0 then
                                                (max(reserved_memory_kb_max_value - declared_size_memory_kb_start_value)/1024/1024)::integer 
                                        else 0 END as reservedsize,
                                CASE WHEN (max(reserved_memory_kb_max_value - declared_size_memory_kb_start_value)/1024/1024)::integer  > 0 then
                                		(max(reserved_memory_kb_max_value - declared_size_memory_kb_start_value)/1024/1024)::integer 
                                	else 0 END as borrowedsize,
                                max(reserved_queries_max_value) as no_queries
                                FROM dc_resource_pool_status_by_""" + args.grain + """ 
                                WHERE time > current_date - """ + str(args.days) + """
                                AND pool_name NOT IN """ + str(pool_name_not_in) + """
                                GROUP BY 1,2  ORDER by 1,2 DESC; """)

   rows = cur.fetchall()
   fig,ax = plt.subplots(no_subplots)
   ax_sec = [a.twinx() for a in ax]

   fig.set_figheight( 3 * no_subplots) 

   prior_rp = ""
   xdata,ydata1,ydata2,ydata3 = [],[],[],[]

   for index, row in enumerate(rows):
        #start
        if prior_rp == "":
                 prior_rp = row[0]
                 i = 0
        #during
        if row[0] != prior_rp or index  == len(rows) - 1 : #report based on completion or on last record
                    if index == len(rows) - 1 : #last row to append first before plotting
                       #keep the same plot and add a new data point
                       xdata.append(row[1])	  # hour
                       ydata1.append(int(row[2])) # reserved
                       ydata2.append(int(row[3])) # borrowed memory
		       ydata3.append(int(row[4])) # concurrency

                    #line, = ax[i].plot(xdata,ydata1,"-",label="Borrowed mem",linewidth=2)
	            #ax[i].plot(xdata,ydata2,"-",label="Max mem",linewidth=2)
                    ax[i].stackplot(xdata,ydata1,ydata2,labels=('Reserved mem','Borrowed mem'))
		    #ax[i].legend((line[0], line[1]), ('Reserved mem','Borrowed mem'))
                    ax[i].set_title(prior_rp + " - " + dict.get(prior_rp, "Missing pool"))
                    ax[i].set_ylabel('Mem(GB)')

                    ax_sec[i].plot(xdata,ydata3,"-",label="Concurrency",linewidth=1,color='g')
                    ax_sec[i].set_ylabel('Concurrency')
                    
                    ax[i].legend(loc=2)
                    ax_sec[i].legend(loc=1)
                    ax[i].grid(True)

                    # format the ticks
		    if args.grain == "minute" :
                    	ax[i].xaxis.set_major_locator(HourLocator())
			ax[i].xaxis.set_major_formatter(DateFormatter('%d-%H:%M'))
		    else : # hour
			ax[i].xaxis.set_major_locator(DayLocator())
                    	ax[i].xaxis.set_major_formatter(DateFormatter('%b %d'))

                    ax[i].xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1,ydata2,ydata3 = [],[],[],[]
                    prior_rp = row[0] #reset rp name
		    if i<no_subplots - 1:
                        i+=1

        #keep the same plot and add a new data point
        xdata.append(row[1])       # hour
        ydata1.append(int(row[2])) # reserved 
        ydata2.append(int(row[3])) # borrowed memory
        ydata3.append(int(row[4])) # concurrency

   plt.savefig("MEM")
   cur.close()

   img = open('MEM.png', 'rb').read()
   msgImg = MIMEImage(img, 'png')
   msgImg.add_header('Content-ID', '<mem>')
   msgImg.add_header('Content-Disposition', 'inline', filename='MEM.png')
   msg.attach(msgImg)


def exec_label(message):
 # ability to check execution time + memory usage + any spilling + retries + any other special event occurred on this pattern of queries
 # can leverage either a set of labels or a sql pattern 
 # add also a report where we pass in a transaction_id / statement_id and get back EEP data , planning data , query plan in table + chart format 
 global args
 labellist = "'kpis_mobile_ltv', 'kpis_canvas_adacq'"
 pattern = "...."
 cur = db.cursor()
 cur.execute("set session timezone ='America/New_York';")
 
 cur = db.cursor()
 cur.execute("""select  label, s.time as starttime,
        datediff('second',s.time,f.time) as duration,
        sub.mem_gb
	from """ + args.dcschema + """.requests_issued s  inner join """+args.dcschema +""".requests_completed f using (session_id,request_id)
	left outer join (select transaction_id,
                statement_id,
                pool_name,
                (max(memory_kb)/1024/1024)::integer  as mem_gb ,
                min(time) start_time,
                max(threads) threads,
                max(filehandles) FH
        FROM """+ args.dcschema + """.resource_acquisitions A
        where A.time >= current_date - """ + str(args.days) + """
        GROUP BY 1,2,3 ORDER BY 4 DESC) SUB ON SUB.transaction_id = s.transaction_id and SUB.statement_id = s.statement_id
	where f.time >= current_date - """ + str(args.days) + """ 
	and s.time >= current_date - """ + str(args.days) + """
	and  label IN (""" + str(labellist)+ """)
	order by  label ASC , s.time DESC ; """ )

 rows = cur.fetchall()
 fig, ax = plt.subplots(1)
 ax_sec = ax.twinx()

 prior_label,sti  =  "",1

 xdata,ydata1,ydata2 =  [],[],[]
 for index, row in enumerate(rows):
        #start
        if prior_label == "":
                 prior_label = row[0]
        #during
        if row[0] != prior_label or index  == len(rows) - 1 : #report
		    if index == len(rows) - 1 : #last row to append first before plotting
                       #keep the same plot and add a new data point
                       xdata.append(row[1])       # hour
                       ydata1.append(int(row[2])) # exec time
                       ydata2.append(int(row[3])) # mem usage

                    style = "o"
                    line, = ax.plot(xdata,ydata1,"-",label= prior_label + "-sec")
                    ax.set_title('Labeled Queries time/memory usage')
                    ax.set_ylabel('Time (secs) ')
                    ax.legend(loc=2,prop={'size':7})

                    ax_sec.plot(xdata,ydata2,":",color=line.get_color(),label=prior_label + "-GB")
                    ax_sec.set_ylabel('Mem(GB)')
		    ax_sec.legend(loc=1,prop={'size':7})

                    ax.grid(True)
		    ax.set_xlabel('Date')

                    # format the ticks
                    ax.xaxis.set_major_locator(DayLocator())
                    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
                    ax.xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1,ydata2 = [],[],[]
                    prior_label = row[0]
                    sti += 1

        #keep the same plot and add a new data point
        xdata.append(row[1])
        ydata1.append(int(row[2])) # exec time
        ydata2.append(int(row[3])) # mem usage

 
 plt.savefig("LABEL")
 cur.close()

 img = open('LABEL.png', 'rb').read()
 msgImg = MIMEImage(img, 'png')
 msgImg.add_header('Content-ID', '<label>')
 msgImg.add_header('Content-Disposition', 'inline', filename='LABEL.png')
 msg.attach(msgImg)


def exec_spilled(message):
 global args

 cur = db.cursor()
 cur.execute("set session timezone ='America/New_York';")
 
 cur = db.cursor()
 cur.execute("""SELECT	S.event_type || '-' || A.pool_name,
        	date_trunc('hour',S.time)::timestamp,
        	count(distinct S.session_id || '' || S.transaction_id || '' ||S.statement_id) as spill_count,
        	max(A.mem_gb)::numeric(14,2) as max_mem_gb
		FROM
        		(select time, user_name, event_type,session_id,transaction_id,statement_id
                	from dc_execution_engine_events where time >= current_date -""" + str(args.days) + """ 
                	and event_type IN ('GROUP_BY_SPILLED', 'JOIN_SPILLED') ) S
			INNER JOIN (select      transaction_id,
                        			statement_id,
                        			pool_name,
                        		(max(memory_kb)/1024/1024)::numeric(14,2) as mem_gb
                			FROM """+ args.dcschema + """.resource_acquisitions where time >= current_date -""" +str(args.days)+ """ group by 1,2,3) A
					ON S.transaction_id = A.transaction_id AND S.statement_id = A.statement_id
					WHERE A.mem_gb > 50  -- greater than 50 GB 
					group by 1,2 order by 1,2 DESC;""")
 rows = cur.fetchall()
 #make 2 subplots, 1 for query count and one for mem usage
 fig,ax = plt.subplots(1)
 ax_sec = ax.twinx()
 
 prior_rp,sti  =  "",1
 xdata,ydata1,ydata2 =  [],[],[]
 for index,row in enumerate(rows):
        #start
        if prior_rp == "":
                 prior_rp = row[0]
        #during
        if row[0] != prior_rp or index  == len(rows) - 1 : #report
		    if index == len(rows) - 1 : #last row to append first before plotting
			#keep the same plot and add a new data point
 		       	xdata.append(row[1])
		        ydata1.append(int(row[2])) # query count
		        ydata2.append(int(row[3])) # mem usage

                    line, =ax.plot(xdata,ydata1,"-",label= prior_rp,linewidth=2)
                    ax.set_title('Join/GroupBy SPILL - Query count/Mem usage')
                    ax.set_ylabel('Query Count')
		    ax.legend(loc=2,prop={'size':7})

                    ax_sec.plot(xdata,ydata2,":",label=prior_rp ,linewidth=2)
                    ax_sec.set_ylabel('Mem(GB)')
		    ax_sec.legend(loc=1,prop={'size':7})
                    
		    ax.set_xlabel('Date')
                    ax.grid(True)

                    # format the ticks
                    ax.xaxis.set_major_locator(DayLocator())
                    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
                    ax.xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1,ydata2 = [],[],[]
                    prior_rp = row[0]
                    sti += 1
        #keep the same plot and add a new data point
        xdata.append(row[1])
        ydata1.append(int(row[2])) # query count
        ydata2.append(int(row[3])) # mem usage

 plt.savefig("SPILL")
 cur.close()

 img = open('SPILL.png', 'rb').read()
 msgImg = MIMEImage(img, 'png')
 msgImg.add_header('Content-ID', '<spill>')
 msgImg.add_header('Content-Disposition', 'inline', filename='SPILL.png')
 msg.attach(msgImg)

def exec_gcl(message):
 # combine the lock attempts and lock releases to also show hold time and max hold time 
 global args
 cur = db.cursor()
 cur.execute("set session timezone ='America/New_York';")
 cur = db.cursor()
 cur.execute("""select 	X.hour, 
		X.wait_count,
		X.max_wait_sec, 
		Y.max_hold_sec,
		X.lock_count,
		X.avg_wait_sec,
		Y.avg_hold_sec
		FROM
			(select date_trunc('hour',time)::timestamp as hour, 
			count(*) as lock_count,
			sum(case when description='Granted immediately' then 0 else 1 end) as wait_count,
			avg(datediff('ss',start_time,time))::numeric(12,5) as avg_wait_sec,
			max(datediff('ss',start_time,time)) as max_wait_sec
			FROM dc_lock_attempts
				where object_name  IN ('Global Catalog') 
				and time >= (current_date - """ + str(args.days) + """)
				and regexp_like(node_name,'node0001$')
			group by 1) X left outer join 
			(select date_trunc('hour',time)::timestamp as hour, 
			avg(datediff('ss',grant_time,time))::numeric(12,5) as avg_hold_sec,
 			max(datediff('ss',grant_time,time)) as max_hold_sec
			FROM  dc_lock_releases
			where object_name  IN ('Global Catalog') 
			and time >= (current_date - """+ str(args.days) +""")
			and regexp_like(node_name,'node0001$')
			group by 1) Y using (hour) order by x.hour ASC;""" )
 rows = cur.fetchall()
 #make 2 subplots, * for max wait / max hold and wait lock count 
 #		   * fro avg wait / avg hold and lock count
 fig,ax = plt.subplots(2)
 ax_sec  = [a.twinx() for a in ax]

 xdata,ydata1,ydata2,ydata3,ydata4,ydata5,ydata6 =  [],[],[],[],[],[],[]

 for index, row in enumerate(rows):
        #during
        if  index == len(rows) -1 : #report
                    #keep the same plot and add a new data point
		    #subplot - 1 
                    xdata.append(row[0])  # time
                    ydata1.append(row[1]) # Wait count
                    ydata2.append(row[2]) # Max wait sec
                    ydata3.append(row[3]) # Max hold sec
		    #subplot - 2
                    ydata4.append(row[4]) # lock count
                    ydata5.append(row[5]) # avg wait 
		    ydata6.append(row[6]) # avg hold

		    #plot 1 
                    ax[0].plot(xdata,ydata2,"-",label="wait max")
                    ax[0].plot(xdata,ydata3,"-",label="hold max")
                    
		    ax[0].set_title('GCL Maximum Wait&Hold time / Wait lock count')
                    ax[0].set_ylabel('Wait&Hold GCL(sec)')

                    ax_sec[0].plot(xdata,ydata1,":",label="wait count")
                    ax_sec[0].set_ylabel('Wait lock count')

                    ax[1].plot(xdata,ydata5,"-",label="wait avg")
		    ax[1].plot(xdata,ydata6,"-",label="hold avg")
                    ax[1].set_title('GCL Average Wait&Hold Time / Lock count')
                    ax[1].set_ylabel('Wait&Hold GCL(sec)')

                    ax_sec[1].plot(xdata,ydata4,":",label="lock count")
                    ax_sec[1].set_ylabel('Lock count')

                    for i in [0,1]:
                       ax_sec[i].legend(loc=1)
                       ax[i].legend(loc=2)
                       ax[i].set_xlabel('Date')
                       ax[i].grid(True)
		       #ax[i].set_yscale("log", nonposx='clip')

                       # format the ticks
                       ax[i].xaxis.set_major_locator(DayLocator())
                       ax[i].xaxis.set_major_formatter(DateFormatter('%b %d'))
                       ax[i].xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1,ydata2,ydata3,ydata4,ydata5,ydata6 = [],[],[],[],[],[],[]

        #keep the same plot and add a new data point
	#subplot - 1 
        xdata.append(row[0])  # time
        ydata1.append(row[1]) # Wait count
        ydata2.append(row[2]) # Max wait sec
        ydata3.append(row[3]) # Max hold sec
        #subplot - 2
        ydata4.append(row[4]) # lock count
        ydata5.append(row[5]) # avg wait 
        ydata6.append(row[6]) # avg hold

 plt.tight_layout()
 plt.savefig("GCL")
 cur.close()

 img = open('GCL.png', 'rb').read()
 msgImg = MIMEImage(img, 'png')
 msgImg.add_header('Content-ID', '<gcl>')
 msgImg.add_header('Content-Disposition', 'inline', filename='GCL.png')
 msg.attach(msgImg)

def exec_reswait(msg):
   global args 
   pool_name_not_in ="('dbd','jvm','recovery','refresh')"
   #get maxconcurrency to plot in graphs
   cur = db.cursor()
   cur.execute("set session timezone ='America/New_York';")
 
   cur=db.cursor()
   cur.execute("select name,memorysize,maxmemorysize,plannedconcurrency,maxconcurrency  from resource_pools where name NOT IN " + pool_name_not_in + ";")
   rows = cur.fetchall()

   dict ={}
   for row in rows:
      dict[str(row[0])] = str(row[1]) + "/" + str(row[2]) + "/" + str(row[3]) + "/" + str(row[4])
   cur.close()

   no_subplots = len(rows)

   cur = db.cursor()
   cur.execute("""select pool_name || '-' || result_type,
		date_trunc('hour',time)::timestamp  as hour,
		count(*) as queries,
		max(max_requested) as mem_requested
		FROM ( SELECT  pool_name, 
		transaction_id || '-' || statement_id,
		case when regexp_like(result,'Request exceeds limits') then 'Rejected'
			when regexp_like(result,'Timedout waiting for resource request') then 'Unable2borrow'
 			when regexp_like(result,'General cannot supply required overflow resources') then 'Unable2borrow'
			when regexp_like(result,'Request for resource was canceled') then 'Unable2borrow'
 			when regexp_like(result,'Request Too Large:Memory\(KB\) Exceeded') then 'Rejected'
 			else result 
		end as result_type,
		min(time) as time,
		--max(datediff('second',start_time,time))::integer as queue_secs_max
		(max(memory_kb)/1024/1024)::numeric(9,2) as max_requested
		FROM """+ str(args.dcschema)  +""".resource_acquisitions 
		WHERE  time > current_date - """ + str(args.days) + """
                AND pool_name NOT IN """ + str(pool_name_not_in) + """
		GROUP BY 1,2,3 ) T
		where result_type not in ('Granted') 
		GROUP BY 1,2 order by 1,2 DESC;""")
   rows = cur.fetchall()
   fig,ax = plt.subplots(1)
   ax_sec = ax.twinx() 

   prior_rp = ""
   xdata,ydata1,ydata2 = [],[],[]

   for index, row in enumerate(rows):
        #start
        if prior_rp == "":
                 prior_rp = row[0]
        #during
        if row[0] != prior_rp or index  == len(rows) - 1 : #report based on completion or on last record
                    if index == len(rows) - 1 : #last row to append first before plotting
                       #keep the same plot and add a new data point
                       xdata.append(row[1])       # hour
                       ydata1.append(int(row[2])) # queries_count
                       ydata2.append(int(row[3])) # queuewait(secs)

		    line, = ax.plot(xdata,ydata1,"-",label=prior_rp,linewidth=1 )
		    ax_sec.plot(xdata,ydata2,":",label=prior_rp,color=line.get_color())

                    ax.legend(loc=2,prop={'size':7})
                    #ax_sec.legend(loc=1,prop={'size':7})
                    ax.grid(True)

                    ax.set_xlabel('Date')
		    ax.set_ylabel('Rejection count')
		    ax_sec.set_ylabel('MemRequested(GB)')	
                    ax.set_title(" Queries Count(#)/MemRequested")

                    # format the ticks
                    ax.xaxis.set_major_locator(DayLocator())
                    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
                    ax.xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1,ydata2 = [],[],[]
                    prior_rp = row[0]

        #if row[0] != prior_rp: # RP has changed, increment counter 
        #         i+=1 
        #         prior_rp = row[0] #reset cat name

        #keep the same plot and add a new data point
        xdata.append(row[1])
        ydata1.append(int(row[2]))
        ydata2.append(int(row[3]))

   plt.savefig("RESWAIT")
   cur.close()

   img = open('RESWAIT.png', 'rb').read()
   msgImg = MIMEImage(img, 'png')
   msgImg.add_header('Content-ID', '<reswait>')
   msgImg.add_header('Content-Disposition', 'inline', filename='RESWAIT.png')
   msg.attach(msgImg)

def exec_objlock (msg):
  global args
  cur = db.cursor()
  cur.execute("set session timezone ='America/New_York';")
 
  cur = db.cursor()
  if args.tbname is None:
     print "For this call a table name was expected, none provided"
     return

  cur.execute("""select  mode,
        	date_trunc('hour',time)::timestamp as hour,
		count(*) ,
        	max(datediff('ss',start_time,time)) 
 		FROM  dc_lock_attempts
		WHERE regexp_like(object_name,'""" + str(args.tbname) + """') 
		and time >= (current_date - """ + str(args.days) + """)
		and datediff('ss',start_time,time) > 0 
		group by 1,2 order by 1,2 ;""")
  
  rows = cur.fetchall()
  fig,ax = plt.subplots(1)
  ax_sec = ax.twinx()

  mode = ""
  xdata,ydata1,ydata2 = [],[],[]

  for index, row in enumerate(rows):
        #start
        if mode == "":
                 mode = row[0]
                 i = 0
        #during
        if row[0] != mode or index  == len(rows) - 1 : #report based on completion or on last record
                    if index == len(rows) - 1 : #last row to append first before plotting
    		       #keep the same plot and add a new data point
                       xdata.append(row[1])       # hour
		       ydata1.append(int(row[2])) # lock count
                       ydata2.append(int(row[3])) # max wait

                    line, = ax.plot(xdata,ydata1,"-",label=mode + " - lock count")
                    ax.set_title(args.tbname + " - lock counts / waits")
                    ax.set_ylabel('Lock count')
		    ax.legend(loc=2,prop={'size':7})

		    ax_sec.plot(xdata,ydata2,":",color=line.get_color(),label=mode + " - lock wait")
		    ax_sec.set_ylabel('Lock wait(sec)')
		    ax_sec.legend(loc=1,prop={'size':7})                    
			
                    ax.grid(True)

                    # format the ticks
                    ax.xaxis.set_major_locator(DayLocator())
                    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
                    ax.xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1,ydata2 = [],[],[]
                    mode = row[0] #reset rp name

        #keep the same plot and add a new data point
        xdata.append(row[1])       # hour
	ydata2.append(int(row[2])) # lock count
        ydata1.append(int(row[3])) # reserved

  plt.savefig("OBJLOCK")
  cur.close()

  img = open('OBJLOCK.png', 'rb').read()
  msgImg = MIMEImage(img, 'png')
  msgImg.add_header('Content-ID', '<objlock>')
  msgImg.add_header('Content-Disposition', 'inline', filename='OBJLOCK.png')
  msg.attach(msgImg)

def getstyle(s):
 if s <=15:
    style = "-"
 elif s >= 15 and s <30:
    style = "-."
 else:
    style = ":"
 return style

#main body of script

parser = argparse.ArgumentParser(description='Create charts for vertica historical performance.')
parser.add_argument('--email',
                    help='email address to send report to')
parser.add_argument('--days', type=int,
                    help='number of days back from present to capture in the report')
parser.add_argument('--type',
                    help='report type to send MEM|LABEL|SPILL|GCL|OBJLOCK|RESWAIT|ALL')
parser.add_argument('--password',
                    help='vertica dbadmin\'s password')
parser.add_argument('--host',
                    help='vertica host to connect to ')
parser.add_argument('--db',
                    help='vertica db name ')
parser.add_argument('--sqlrequest',
                    help='string to use in LABEL report')
parser.add_argument('--labellist',
                    help='list of labels to use in LABEL report')
parser.add_argument('--tbname',
                    help='table name in the OBJLOCK report')
parser.add_argument('--dcschema',
		    help='schema name for data collector schema')
parser.add_argument('--grain',default='hour',
                    help='grain for mem usage : hour/minute, default hour')


args = parser.parse_args()
if args.host == None:
        args.host = "localhost"
if args.days == None:
        args.days = 7 
if args.type == None:
        args.type = "ALL"
if args.password == None:
        args.password = "no_need_for_password"
if args.db == None:
        args.db = "db"
if args.dcschema is None:
        args.dcschema = "dc"

db = hp_vertica_client.connect("host=" + args.host + " database="+ args.db + " port=5433 user=dbadmin password=" + args.password  )
msg = MIMEMultipart('related')

me = "cloghin@bseatech.com"
you = args.email
msg['Subject'] = "Healthcheck charts (EST TZ)- " + str(args.host)+ "-" + str(args.type) + "-" + str(args.days) + " days"
msg['From'] = me
msg['To'] = you

# Create the body of the message.
html = """\
        <p><img src="cid:mem"><BR>
                <img src="cid:gcl"><BR>
                <img src="cid:spill"><BR>
		<img src="cid:label"><BR>
		<img src="cid:reswait"><BR>
		<img src="cid:objlock"><BR>
        </p>"""

# Record the MIME types.
msgHtml = MIMEText(html, 'html')
msg.attach(msgHtml)

# resource pool usage over time ( # queries, reserved_memory) 
if args.type in ['MEM','ALL']:
       exec_memusage(msg)
#labeled queries execution time + memory , last 7 days 
if args.type in ['LABEL','ALL']:
       exec_label(msg)
if args.type in ['SPILL','ALL']:
       exec_spilled(msg)
if args.type in ['GCL','ALL']:
       exec_gcl(msg)
if args.type in ['OBJLOCK','ALL']:
       exec_objlock (msg)
if args.type in ['RESWAIT','ALL']:
       exec_reswait(msg)

db.close()

if args.email is not None:
	# Send the message via our own SMTP server, but don't include the envelope header.
        s = smtplib.SMTP('localhost')
        s.sendmail(me,[you],msg.as_string())
        s.quit()

