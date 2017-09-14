import argparse,sys,os
import hp_vertica_client
from subprocess import call, PIPE,Popen
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
from cycler import cycler
import datetime
import numpy as np

# Import the email modules we'll need
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart


plt.rc('axes', prop_cycle=(cycler('color', ['#e41a1c', '#377eb8', '#4daf4a', '#ff7f00', '#a65628', '#f781bf', '#999999', '#984ea3', '#ffff33','#7fff00',
                                            '#F0A3FF', '#0075DC', '#993F00', '#4C005C', '#191919']) ))

font = {'size': 8}
plt.rc('font', **font)

def exec_memlarge2(message):
   global args
   cur=db.cursor()
   cur.execute("set session timezone ='America/New_York';")
   cur=db.cursor()

   sql =""" SELECT ATABLE.result_type ||'-'|| case CTABLE.success when 't' then 'Retried&OK' else 'Failed' END as result,
                   ATABLE.pool_name,
                   ATABLE.transaction_id,
                   ATABLE.START,
                   ATABLE.mem_gb
                   FROM ( SELECT pool_name, transaction_id,
                          CASE WHEN regexp_like(result,'Granted') then 'Granted'
                               ELSE 'NotGranted'
                               END as result_type,
                        max(date_trunc('second',start_time))::timestamp as start,
                        max(datediff('second',start_time,time)) as wait_sec,
                        (max(memory_kb/1024/1024))::numeric(14,2) as mem_GB
                        FROM """+ str(args.dcschema)  +""".resource_acquisitions
                        WHERE  time >= current_date - """+ str(args.days)  +"""
                        GROUP BY  pool_name, transaction_id, result_type
                        ) ATABLE
                        INNER JOIN
                        (SELECT transaction_id, max(statement_id)  statement_id, max(success::varchar) AS success
                                FROM """+ str(args.dcschema)  +""".query_summary_hist
                                WHERE date(start_timestamp) > current_date -1 - """+ str(args.days)+"""
                                AND request_type NOT IN ('TRANSACTION') group by 1
                        ) CTABLE USING (transaction_id)
                        WHERE ATABLE.result_type <> 'Granted' -- MEMORY ERRORS

                        UNION ALL

                        SELECT 'Granted', A.*  FROM
                        ( SELECT pool_name,
                                 transaction_id,
                                 max(date_trunc('second',start_time))::timestamp AS start,
                                 (max(memory_kb/1024/1024))::numeric(14,2) AS mem_GB
                           FROM """+ str(args.dcschema)  +""".resource_acquisitions
                           WHERE  time >= current_date - """+ str(args.days)  +"""
                           AND RESULT ='Granted'                -- MEMORY GRANTS > BUDGET by 50%.
                           GROUP BY  pool_name, transaction_id
                        ) A
                        INNER JOIN (
                                select pool_name, avg(query_budget_kb/1024/1024)::numeric(14,2) as budget from resource_pool_status group by 1
                                ) B  using(pool_name)
                        WHERE A.mem_gb > B.budget * 1.5 AND A.mem_GB > 1 ;"""
   #print sql
   cur.execute(sql)

   rows = cur.fetchall()
   points = []
   for row in rows:
                points.append(row)
   cur.close()

   # get number of subplots based on distinct pool_name(s)
   pools = list(set([item[1] for item in points]))
   no_subplots = len(pools)

   fig,ax = plt.subplots(no_subplots)
   fig.suptitle("Memory Rejections (red) & workloads > 1.5 * pool_budget  (green) ", fontsize=12)

   fig.set_figheight( 3  * no_subplots)
   fig.set_figwidth (15)

   i=0
   for pool in sorted(pools):
        l = [item for item in points if item[1] == pool]
        ax[i].grid(True)
        ax[i].set_title(pool,y=0.80)
        ax[i].set_ylabel('Mem(GB)')
        ax[i].xaxis.set_major_locator(HourLocator(np.arange(0, 25, 6)))
        ax[i].xaxis.set_major_formatter(DateFormatter('%m/%d-%H'))
        ax[i].xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))
        ax[i].set_xlim([datetime.date.today() - datetime.timedelta(days=args.days, hours=1  ) ,  datetime.date.today() +  datetime.timedelta(hours=1) ])
        plt.setp(ax[i].get_xticklabels(), rotation=35)
        for stat in list(set([item[0] for item in l])):
                #print stat
                if stat == 'Granted':
                        style='go'
                elif stat == 'NotGranted-Failed':
                        style='ro'
                else:
                        style='yo'
                l2 = [item for item in l if item[0] == stat] # build list for each stat (granted, retried, failed) 
                l3 = [i1[3] for i1 in l2] # x axis, dates
                l4 = [i2[4] for i2 in l2] # y axis, memory
                ax[i].plot(l3,l4,style,label= stat,markersize=5)
                ax[i].set_ylim(0,1.5 * max(l4))
                ax[i].legend(loc=2,prop={'size':7})
        i += 1
   plt.savefig("MEMLARGE")
   img = open('MEMLARGE.png', 'rb').read()
   msgImg = MIMEImage(img, 'png')
   msgImg.add_header('Content-ID', '<memlarge>')
   msgImg.add_header('Content-Disposition', 'inline', filename='MEMLARGE.png')
   msg.attach(msgImg)

def exec_wait(msg):
   global args
   cur=db.cursor()
   cur.execute("set session timezone ='America/New_York';")


   cur=db.cursor()
   cur.execute(""" select count( DISTINCT pool_name)   FROM dc_resource_acquisitions
                        WHERE  time >= current_date - """+ str(args.days)  +"""
                        AND RESULT = 'Granted'
                        AND datediff('second',start_time,time) > 2; """)

   row = cur.fetchone()
   no_subplots = row[0]

   cur=db.cursor()
   cur.execute(""" select pool_name ,max(date_trunc('second',time))::timestamp
                        ,max(datediff('second',start_time,time))
                        FROM dc_resource_acquisitions
                        WHERE  time >= current_date - """+ str(args.days)  +"""
                        AND RESULT = 'Granted' 
                        GROUP BY  pool_name, transaction_id, statement_id
                        HAVING max(datediff('second',start_time,time)) > 2
                        ORDER BY 1,2;""")
   rows = cur.fetchall()
   fig,ax = plt.subplots(no_subplots)

   fig.set_figheight( 3 * no_subplots)
   fig.set_figwidth (10)


   prior_rp = ""
   xdata,ydata1 = [],[]

   for index, row in enumerate(rows):
        #start of iterations
        if prior_rp == "":
                 prior_rp = row[0]
                 i = 0
        #during
        if row[0] != prior_rp or index  == len(rows) - 1 : #report based on completion or on last record
                    if index == len(rows) - 1 : #last row to append first before plotting
                       #keep the same plot and add a new data point
                       xdata.append(row[1])       # time
                       ydata1.append(int(row[2])) # queue_sec_max

                    line, = ax[i].plot(xdata,ydata1,"ro",label=prior_rp,linewidth=1 )

                    ax[i].legend(loc=2,prop={'size':7})
                    ax[i].grid(True)

                    #ax[i].set_xlabel('Date')
                    ax[i].set_ylabel('Wait time(sec)')
                    #ax[i].set_title("Wait time / Pool")

                    # format the ticks
                    ax[i].xaxis.set_major_locator(DayLocator())
                    ax[i].xaxis.set_major_formatter(DateFormatter('%b %d'))
                    #datetime.strptime('2017-05-08 09:04:05', "%Y-%m-%d %H:%M:%S").date() - timedelta(days=2) 
                    #print "****", type(min(xdata)) , "***"
                    #ax[i].set_xlim( [ min(xdata),max(xdata) ] )
                    ax[i].set_xlim([datetime.date.today() - datetime.timedelta(days= args.days + 1  ) ,  datetime.date.today() +  datetime.timedelta(days=1) ])
                    ax[i].xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))

                    xdata,ydata1 = [],[]
                    prior_rp = row[0] #reset rp name
                    if i< no_subplots - 1:
                        i+=1

        #keep the same plot and add a new data point
        xdata.append(row[1])
        ydata1.append(int(row[2]))

   plt.savefig("WAIT")
   cur.close()

   img = open('WAIT.png', 'rb').read()
   msgImg = MIMEImage(img, 'png')
   msgImg.add_header('Content-ID', '<wait>')
   msgImg.add_header('Content-Disposition', 'inline', filename='WAIT.png')
   msg.attach(msgImg)


def exec_memlarge(message):
   global args
   cur=db.cursor()
   cur.execute("set session timezone ='America/New_York';")

   cur=db.cursor()

   #get pools that had memory rejections + pool budget , need to count the subplots
   cur.execute(""" select DISTINCT B.pool_name, B.budget  from 
        (select pool_name,transaction_id from dc_resource_acquisitions where result <>'Granted' and date(time) > current_date -"""+ str(args.days)+""" and pool_name not in ('wosdata','sysquery') ) A 
        inner join (select pool_name, avg(query_budget_kb/1024/1024)::numeric(14,2) as budget from resource_pool_status group by 1) B using(pool_name)
        inner join (select session_id,transaction_id FROM """+ str(args.dcschema)  +""".query_summary_hist where date(start_timestamp) > current_date - """+ str(args.days)  +""" group by 1,2 having max(success::VARCHAR) = 'f')
        QR using (transaction_id)""")

   rows = cur.fetchall()
   dict ={}
   pool_list=""
   for row in rows:
      dict[str(row[0])] = row[1]
      pool_list += "'" + str(row[0]) + "',"
   cur.close()
   no_subplots = len(rows) #cannot add subplots dynamically  so we need to count them ahead of time

   print pool_list #debug 

   cur=db.cursor()
   cur.execute("""select pool_name,
                        date_trunc('second',time)::timestamp as time_sec, 
                        (memory_kb/1024/1024)::numeric(14,2) as GB  
               FROM (select row_number() over (partition by DRA.transaction_id order by DRA.memory_kb DESC) rn ,
                        DRA.transaction_id, 
                        DRA.memory_kb,
                        DRA.time,
                        DRA.result,
                        DRA.pool_name from
                 """+ str(args.dcschema)  +""".resource_acquisitions DRA 
                 where DRA.pool_name IN (""" + pool_list[:-1] + """) 
                 and date(time) > current_date -"""+ str(args.days)+""" 
                  and result <> 'Granted') A 
                 inner join (select session_id,transaction_id from """+ str(args.dcschema)  +""".query_summary_hist  
                  where date(start_timestamp) > current_date - """+ str(args.days)  +"""  group by 1,2 having max(success::VARCHAR) = 'f')  QR using (transaction_id)
                 where A.rn = 1 order by pool_name,time_sec;""")

   rows = cur.fetchall()
   rej_dict ={}
   prior_rp=rows[0][0]
   xdata2,ydata2 = [],[]
   for row in rows:
      if row[0] <> prior_rp :
        rej_dict[prior_rp] = (xdata2,ydata2)
        xdata2,ydata2 = [],[] # reset lists 
        prior_rp=row[0] #reset prior_rp 
      #add new data point 
      xdata2.append(row[1])
      ydata2.append(float(row[2])) # memory granted 

   #last record add to rej_dict 
   rej_dict[prior_rp] = (xdata2,ydata2)

   cur.close()


   cur = db.cursor()
   cur.execute("""select pool_name, 
                date_trunc('second',time)::timestamp as time_sec, 
               (memory_kb/1024/1024)::numeric(14,2) as GB 
               FROM (select row_number() over (partition by DRA.transaction_id order by DRA.memory_kb DESC) rn ,
                        DRA.transaction_id, 
                        DRA.memory_kb,
                        DRA.time,
                        DRA.result,
                        DRA.pool_name from
                """+ str(args.dcschema)  +""".resource_acquisitions DRA inner join (select pool_name, avg(query_budget_kb/1024/1024)::numeric(14,2) budget from resource_pool_status group by 1) B using(pool_name)
                 where DRA.pool_name in (""" + pool_list[:-1] + """)
                 and result = 'Granted' and  DRA.memory_kb/1024/1024 > B.budget
                 and date(time) > current_date -"""+ str(args.days)+""" 
                ) A where A.rn = 1 order by pool_name,time_sec;""")
   rows = cur.fetchall()

   fig,ax = plt.subplots(no_subplots)
   #ax_sec = [a.twinx() for a in ax]

   fig.set_figheight( 2 * no_subplots)
   fig.set_figwidth (30)

   prior_rp = "" # resource pool name
   xdata1,ydata1 = [],[]


   for index, row in enumerate(rows):
      #start of iterations
        if prior_rp == "":
                 prior_rp = row[0]
                 i = 0
        #during
        if row[0] != prior_rp or index  == len(rows) - 1 : #report based on completion or on last record
                    if index == len(rows) - 1 : #last row to append first before plotting
                       #keep the same plot and add a new data point
                       xdata1.append(row[1])
                       ydata1.append(float(row[2])) # memory granted 

                    ax[i].plot(xdata1,ydata1,"go",label="Granted",markersize=3)
                    ax[i].set_title(prior_rp ,y=0.80)
                    ax[i].set_title(prior_rp + " - " + str(dict.get(prior_rp, "-1")) +" Mem budget(GB)" ,y=0.80)

                    ax[i].set_ylabel('Mem(GB) Granted')
                    (xdata2,ydata2) = rej_dict.get(prior_rp,((),()))
                    print prior_rp
                    print xdata2
                    print ydata2

                    #right axs graph - regular plot
                    ax[i].plot(xdata2,ydata2,"sr",markersize=7)
                    #ax_sec[i].bar(xdata2, ydata2, width, color='b')
                    #ax_sec[i].set_ylabel('Mem(GB) Not Granted')

                    #ax[i].legend(loc=2)
                    #ax_sec[i].legend(loc=1)
                    ax[i].grid(True)
                    ax[i].axhline(dict.get(prior_rp, 0) ,color='g')

                    # format the ticks
                    ax[i].xaxis.set_major_locator(HourLocator(np.arange(0, 25, 6)))
                    ax[i].xaxis.set_major_formatter(DateFormatter('%m/%d-%H:'))
                    ax[i].xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))
                    #ax[i].set_xlim([datetime.date(2017, 4, 5), datetime.date(2017, 4, 13)])

                    xdata1,ydata1 = [],[]
                    prior_rp = row[0] #reset rp name
                    if i<no_subplots - 1:
                        i+=1

        #keep the same plot and add a new data point
        xdata1.append(row[1])
        ydata1.append(float(row[2])) # memory granted 

   plt.savefig("MEMLARGE")
   cur.close()

   img = open('MEMLARGE.png', 'rb').read()
   msgImg = MIMEImage(img, 'png')
   msgImg.add_header('Content-ID', '<memlarge>')
   msgImg.add_header('Content-Disposition', 'inline', filename='MEMLARGE.png')
   msg.attach(msgImg)


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
  
   ax[i].legend(loc=2)
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
 threshold = '20'  #show spills > threshold GB only 

 cur = db.cursor()
 cur.execute("set session timezone ='America/New_York';")
 
 cur = db.cursor()
 cur.execute("""SELECT A.pool_name,
		       S.event_type,
        	       date_trunc('hour',S.time)::timestamp,
        	count(distinct S.session_id || '' || S.transaction_id || '' ||S.statement_id) as spill_count,
        	max(A.mem_gb)::numeric(14,2) as max_mem_gb
		FROM  (SELECT 	time, 
				user_name, 
				event_type,
				session_id,
				transaction_id,
				statement_id
                	FROM  dc_execution_engine_events
			WHERE time >= current_date -""" + str(args.days) + """ 
                	AND event_type IN ('GROUP_BY_SPILLED','JOIN_SPILLED') 
			) S
			INNER JOIN (SELECT transaction_id,
                        		   statement_id,
                        		   pool_name,
                        		   (max(memory_kb)/1024/1024)::numeric(14,2) as mem_gb
                			FROM """+ args.dcschema + """.resource_acquisitions
					WHERE time >= current_date -""" + str(args.days)+ """ group by 1,2,3
				   ) A
			USING (transaction_id, statement_id) 	
			WHERE A.mem_gb > """ + threshold +"""  -- greater than 'threshold' GB
			GROUP BY 1,2,3 order by 1,2,3;""")

 if (cur.rowcount == 0 ):
  return
 
 rows = cur.fetchall()
 points = []
 for row in rows:
                points.append(row)
 cur.close()

 # get number of subplots based on distinct pool_name(s)
 pools = list(set([item[0] for item in points]))
 no_subplots = len(pools)
 
 fig,ax = plt.subplots(no_subplots)
 ax_sec = [a.twinx() for a in ax]
 fig.suptitle("Join/GroupBy SPILL(>"+ threshold + " GB)  - Query count/Mem usage", fontsize=12)

 fig.set_figheight( 3  * no_subplots)
 fig.set_figwidth (15)
 
 for i,pool in enumerate(sorted(pools)):
        l = [item for item in points if item[0] == pool] #list of rows for a given pool 
        ax[i].grid(True)
        ax[i].set_title(pool,y=0.80)
        ax[i].set_ylabel('Spilled Mem(GB)')
        ax[i].xaxis.set_major_locator(HourLocator(np.arange(0, 25, 6)))
        ax[i].xaxis.set_major_formatter(DateFormatter('%m/%d-%H'))
        ax[i].xaxis.set_minor_locator(HourLocator(np.arange(0, 25, 6)))
        ax[i].set_xlim([datetime.date.today() - datetime.timedelta(days=args.days, hours=1  ) ,  datetime.date.today() +  datetime.timedelta(hours=1) ])
        plt.setp(ax[i].get_xticklabels(), rotation=35)
        for event in list(set([item[1] for item in l])):
                l2 = [item for item in l if item[1] == event] # build list for a given event ( GBYSPILLED or JOINSPILL really ) 
                x  = [i1[2] for i1 in l2]
                y1 = [i2[4] for i2 in l2]
		y2 = [i3[3] for i3 in l2]
                ax[i].plot(x,y1,'ro',label=event ,)
                ax[i].legend(loc=2,prop={'size':7})
                
		ax_sec[i].plot(x,y2,"g*",label=event ,markersize=5)
                ax_sec[i].set_ylabel('Spill count')
                ax_sec[i].legend(loc=1,prop={'size':7})

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
   fig,ax = plt.subplots()
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


def exec_bucket(msg):
 global args
 threshold = "501" 

 cur = db.cursor()
 cur.execute("set session timezone ='America/New_York';")
 
 cur = db.cursor() 
 cur.execute(""" SELECT  users.resource_pool, to_char(A.dt,'MMdd') || '-' || A.elapsed_bucket, sum(A.count) FROM
	                       (SELECT
	                       	 date(RI.time) AS dt,
	                       	 RI.user_name ,
	                       	  	CASE WHEN datediff('second',RI.time,RC.time)  < 2 THEN 'A<2s'
	                       				  WHEN datediff('minute',RI.time,RC.time)  < 1 THEN 'B<1m'
	                       				  WHEN datediff('minute',RI.time,RC.time)  < 2 THEN 'C<2m'
	                       				  WHEN datediff('minute',RI.time,RC.time)  < 5 THEN 'D<5m'
	                       				  WHEN datediff('minute',RI.time,RC.time)  < 10 THEN 'E<10m'
	                       				 WHEN datediff('minute',RI.time,RC.time)  < 30 THEN 'F<30m'
	                       		ELSE 'G>30m'
	                       		END AS  elapsed_bucket, 
	                       	   count(*) 
	                       	   FROM """+ args.dcschema +""".requests_issued RI INNER JOIN """+args.dcschema+""".requests_completed RC USING(session_id,request_id) 
	                       WHERE RC.success= TRUE  AND RI.request_type  NOT IN ('SET','UTILITY','TRANSACTION')
	                       AND datediff('millisecond',RI.time,RC.time) > """ + threshold + """ 
	                       AND  date(RI.time) >=  current_Date - """ + str(args.days) + """
 	                       GROUP BY 1 , 2 ,3 ) 
			  	A INNER JOIN users USING (user_name)
 	                        GROUP BY 1,2 ORDER BY 1,2""")
 if (cur.rowcount > 0 ):
  	   rows = cur.fetchall()
  	   points = []
  	   for row in rows:
                points.append(row)
 cur.close()

 # get number of subplots based on distinct pool_name(s)
 pools = list(set([item[0] for item in points]))
 no_subplots = len(pools)
 
 fig,ax = plt.subplots(no_subplots)
 ax_sec = [a.twinx() for a in ax]
 fig.suptitle("Query elapsed time (> "+threshold +  " ms) by pool", fontsize=12)

 fig.set_figheight( 6  * no_subplots)
 fig.set_figwidth (15)

 width = 0.35
 
 for i,pool in enumerate(sorted(pools)):
        l = [item for item in points if item[0] == pool] #list of rows for a given pool 
        ax[i].grid(True)
        ax[i].set_title(pool,y=0.80)
        ax[i].set_ylabel('Query count')
        #ax[i].xaxis.set_major_formatter(DateFormatter('%m/%d-%H'))
        x  = [i1[1] for i1 in l]
        y  = [i2[2] for i2 in l]
	ind = np.arange(len(x))
       	rects1 = ax[i].bar(ind + width, y, width)

	for ii,elem in enumerate(x):
		if elem.find("A<") > 0 :
			rects1[ii].set_color('g')
                if elem.find("B<") > 0 :
                        rects1[ii].set_color('c')
                if elem.find("C<") > 0 :
                        rects1[ii].set_color('b')
                if elem.find("D<") > 0 :
                        rects1[ii].set_color('y')
                if elem.find("E<") > 0 :
                        rects1[ii].set_color('m')
	        if elem.find("F<") > 0 :
                        rects1[ii].set_color('r')
                if elem.find("G>") > 0 :
                        rects1[ii].set_color('k')
	       
	ax[i].set_xticks(ind + 0.5 * width)
	ax[i].set_xticklabels(x, rotation=55 ) #rotation = 35

 plt.savefig("BUCKET")
 cur.close()

 img = open('BUCKET.png', 'rb').read()
 msgImg = MIMEImage(img, 'png')
 msgImg.add_header('Content-ID', '<bucket>')
 msgImg.add_header('Content-Disposition', 'inline', filename='BUCKET.png')
 msg.attach(msgImg)

def exec_license (msg):
	global args
	result = []
	#execute an audit every time we execute the report, this may take some 10-20 min. 
	cur = db.cursor()
	cur.execute("""select 'select audit('''||x||''');' FROM (select distinct table_schema as x  from tables UNION ALL select 'gsnmobile.events') T ;""")
	rows = cur.fetchall()
	cur.close()
	for row in rows:
		print row
		# UNCOMMENT the audit execution !!!!!
		cur = db.cursor()
		cur.execute(row[0])
		cur.close()

	#studio breakdown
	cur = db.cursor()
        cur.execute("""	select  ((X.count/TOT.count)* RAW.gb)::numeric(14,2) as raw_gb,
			((X.count/TOT.count)* COMP.gb)::numeric(14,2) as comp_gb
			from 
			( select  count(*) from gsnmobile.events where app_name = 'TriPeaks Solitaire' ) X 
		CROSS JOIN  (select size_bytes/1024/1024/1204 as gb from user_audits  where object_schema='gsnmobile' and object_name ='events' and audit_end_timestamp > current_Date -25
			order by audit_end_timestamp DESC limit 1) RAW  
		CROSS JOIN (select sum(used_bytes)/1024/1024/1024 as gb from projection_storage where projection_schema='gsnmobile' and anchor_Table_name='events' ) COMP
		CROSS JOIN (select count(*) from gsnmobile.events ) TOT;""")
	rows = cur.fetchall()
        cur.close()
        for row in rows:
              tripeaks_raw = row[0]
	      tripeaks_comp = row[1]


	cur = db.cursor()
        sql="""select RAW.studio, 
			case RAW.studio WHEN  'Casino Studio' then GB_RAW - """ + str(tripeaks_raw) + """ 
			    		 WHEN 'Tripeaks Studio' then GB_RAW + """ + str(tripeaks_raw) + """
			else GB_RAW end as "Raw(GB)",
			case RAW.studio WHEN  'Casino Studio' then GB_COMP - """ + str(tripeaks_comp) + """ 
                                         WHEN 'Tripeaks Studio' then GB_COMP + """ + str(tripeaks_comp) + """
                        else GB_COMP end as "Compressed(GB)"
			 from (select CASE
                        WHEN object_name  IN ( 'bingoapp','grandcasino','gsncom','gsnmobile','newapi','plumbee') THEN 'Casino Studio'
                        WHEN object_name IN ( 'app_wofs','poker') THEN 'Vegas Studio'
                        WHEN object_name IN ( 'arena','ww') THEN 'Skill Studio'
                        WHEN object_name IN ( 'bash') THEN 'Bingo Studio'
                        WHEN object_name IN ( 'tripeaksapp') THEN 'Tripeaks Studio'
                        ELSE 'Others' END as studio,
                (sum(size_bytes)/1024/1024/1024)::numeric(14,2) as GB_RAW
                FROM (
                        select object_name, max(size_bytes) as size_bytes from user_audits 
                        where date(audit_end_timestamp) =CURRENT_DATE  and  object_type ='SCHEMA'
                        GROUP BY 1 ) X 
                group by 1 ) RAW
                NATURAL JOIN        
                (select CASE
                        WHEN projection_schema IN ( 'bingoapp','grandcasino','gsncom','gsnmobile','newapi','plumbee') THEN 'Casino Studio'
                        WHEN projection_schema IN ( 'app_wofs','poker') THEN 'Vegas Studio'
                        WHEN projection_schema IN ( 'arena','ww') THEN 'Skill Studio'
                        WHEN projection_schema IN ( 'bash') THEN 'Bingo Studio'
                        WHEN projection_schema IN ( 'tripeaksapp') THEN 'Tripeaks Studio'
                        ELSE 'Others' END as studio,
                (sum(used_bytes)/1024/1024/1024)::numeric(14,2) as GB_COMP
                FROM projection_storage 
                group by 1) COMP order by 2 DESC;"""

	cur.execute(sql)

	rows = cur.fetchall()
        cur.close()
        fig,ax = plt.subplots() 

	xdata,ydata1,ydata2  = [],[],[]
	for index,row in enumerate(rows):
        	xdata.append(row[0])       # studio
                ydata1.append(int(row[1])) # raw_gb
               	ydata2.append(int(row[2])) # comp_gb

	vsql_args = ["vsql" ,"-h", args.host, "-U","dbadmin","-w",args.password,"-HXc" ]
	p=Popen(vsql_args + [sql] ,stdout=PIPE)
	(html,err) = p.communicate()
	result.append(html)

	ind = np.arange(index + 1)
	width = 0.45	
	rects1 = ax.barh(ind,ydata1,width,color='r')
	rects2 = ax.barh(ind + width, ydata2, width,color='y')
	# add some text for labels, title and axes ticks
	ax.set_xlabel('Storage (GB)')
	ax.set_title('Space usage by studio')
	ax.set_yticks(ind + width)
	ax.set_yticklabels(xdata)
	ax.legend((rects1[0], rects2[0]), ('License', 'Compressed'))

	autolabel(ax, rects1)
	autolabel(ax, rects2)
        plt.savefig("STUDIO")

	#build studio piechart
	fig, ax = plt.subplots()
	ax.pie(ydata1 , labels=xdata, autopct='%1.1f%%', shadow=True)
	ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	plt.title('License Allocation by Studio', y=1.05)
	plt.savefig("PIE_STUDIO")


	#historical studio report 
	cur =db.cursor()
	sql ="""select Y.studio, Y.dt as audit_date,
			(case Y.studio 	WHEN  'Casino Studio' then GB_RAW - C.tripeaks_events_gb
                			WHEN 'Tripeaks Studio' then GB_RAW + C.tripeaks_events_gb
                    			ELSE GB_RAW END)::numeric(14,2) as "Raw(GB)"
			FROM ( select dt, CASE
                        		WHEN object_name  IN ( 'bingoapp','grandcasino','gsncom','gsnmobile','newapi','plumbee') THEN 'Casino Studio'
                        		WHEN object_name IN ( 'app_wofs','poker') THEN 'Vegas Studio'
                        		WHEN object_name IN ( 'arena','ww') THEN 'Skill Studio'
                        		WHEN object_name IN ( 'bash') THEN 'Bingo Studio'
                        		WHEN object_name IN ( 'tripeaksapp') THEN 'Tripeaks Studio'
                        		ELSE 'Others' END as studio,
                		(sum(size_bytes)/1024/1024/1024)::numeric(14,2) as  GB_RAW
                		FROM ( select date(audit_end_timestamp) as dt, object_name, max(size_bytes) as size_bytes from user_audits 
                        	where object_type ='SCHEMA' GROUP BY 1,2) X GROUP BY 1,2 )  Y
				NATURAL JOIN (	select A.date as dt , A.gb * B.pct as tripeaks_events_gb from (select date(audit_end_timestamp), avg( size_bytes/1024/1024/1204) as gb 
				from user_audits where object_schema='gsnmobile' and object_name ='events' group by 1) A 
				CROSS JOIN 
			( select  (select count(*) from gsnmobile.events where app_name = 'TriPeaks Solitaire')/(select count(*)  from gsnmobile.events) as pct from dual ) B ) C order by 1;"""
	cur.execute(sql)
	rows = cur.fetchall()
	cur.close()
	
	fig,ax = plt.subplots()
        xdata,ydata,cat  = [],[],""
        for index,row in enumerate(rows):
		#start
        	if cat  == "":
                 	cat  = row[0]
		#during
        	if row[0] != cat or index == len(rows) - 1 : #report based on completion or on last record
                    if index == len(rows) - 1 : #last row to append first before plotting
                       #keep the same plot and add a new data point
                       xdata.append(row[1])       # date
                       ydata.append(int(row[2]))  # size 

                    line, = ax.plot(xdata,ydata,"-",label=cat ,linewidth=2)
		    ax.set_title('Historical License usage')
                    ax.set_ylabel('License(GB)')
                    ax.legend(loc=1)

                    # format the ticks
                    ax.xaxis.set_major_locator(DayLocator())
                    ax.xaxis.set_major_formatter(DateFormatter('%b %d'))

                    xdata,ydata = [],[]
                    cat  = row[0] #reset rp name

        	#keep the same plot and add a new data point
        	xdata.append(row[1])      # date
        	ydata.append(int(row[2])) # size
   	
	plt.savefig("STUDIO_HISTORY")

	#print get_html(field_names,xdata,ydata1,ydata2)
        vsql_args = ["vsql" ,"-h", args.host, "-U","dbadmin","-w",args.password,"-HXc" ]
        p=Popen(vsql_args + [sql] ,stdout=PIPE)
        (html,err) = p.communicate()
        result.append(html)

   	#schema breakdown 
	cur = db.cursor()
	sql="""select * from (select object_name as schema,
        	(sum(size_bytes)/1024/1024/1024)::numeric(14,2) as "RAW(GB)"
 		FROM (
                	select object_name, max(size_bytes) as size_bytes from user_audits 
                	where date(audit_end_timestamp) = CURRENT_DATE  and  object_type ='SCHEMA'
                	GROUP BY 1 ) X 
 		group by 1 ) RAW
 		NATURAL JOIN        
		(select projection_schema as schema,
       	 	(sum(used_bytes)/1024/1024/1024)::numeric(14,2) as "COMPRESSED(GB)"
		FROM projection_storage group by 1) COMP order by 2 DESC ;"""
	cur.execute(sql)
	rows = cur.fetchall()
	cur.close()

	fig,ax = plt.subplots()
	fig.set_figheight( 15 )
	xdata,ydata1,ydata2  = [],[],[]
	for index,row in enumerate(rows):
                xdata.append(row[0])       # schema
                ydata1.append(int(row[1])) # raw_gb
                ydata2.append(int(row[2])) # comp_gb

	ind = np.arange(index + 1)
        width = 0.35
        rects1 = ax.barh(ind,ydata1,width,color='r')
        rects2 = ax.barh(ind + width, ydata2, width,color='y')


 	# add some text for labels, title and axes ticks
        ax.set_xlabel('Storage (GB)')
        ax.set_title('Space usage by schema')
        ax.set_yticks(ind + width)
        ax.set_yticklabels(xdata)
        ax.legend((rects1[0], rects2[0]), ('License', 'Compressed'))

	autolabel(ax, rects1)
	autolabel(ax, rects2)
        plt.savefig("SCHEMA")


	vsql_args = ["vsql" ,"-h", args.host, "-U","dbadmin","-w",args.password,"-HXc" ]
        p=Popen(vsql_args + [sql] ,stdout=PIPE)
        (html,err) = p.communicate()
        result.append(html)

      

	#most recent DB audit 
	cur = db.cursor()
	sql=""" select date_trunc('month', audit_start_timestamp)::date,
        		max((usage_percent*100)::numeric(6,2)) as "license_usage(%)",
        		max(database_size_bytes/1024/1024/1024/1024)::numeric(10,2) as "db_size(TB)" from license_audits where audited_data ='Total'
                 	group by 1 order by  1 ASC;"""

	cur.execute(sql)
	rows=cur.fetchall()
	cur.close()

	fig,ax = plt.subplots()
        xdata,ydata1,ydata2  = [],[],[]
        for index,row in enumerate(rows):
                xdata.append(row[0])       # month
                ydata1.append(int(row[1])) # usage % 
                ydata2.append(int(row[2])) # size TB

        ind = np.arange(index + 1)
        rects1 = ax.bar(ind,ydata2,width,color='r',label='Raw data')

	# add some text for labels, title and axes ticks
	ax.xaxis.set_major_locator(DayLocator())
        ax.xaxis.set_major_formatter(DateFormatter('%b-%Y'))

        ax.set_title('License usage ( db size) over time ')
	ax.set_ylabel('DB size (TB)')
        ax.set_xticks(ind + width / 2. )
        ax.set_xticklabels(xdata, rotation=45)
	ax.legend(loc=2)
	
	ind = 0 
	for rect in rects1:
        	height = rect.get_height()
       		ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%s' % str(ydata2[ind]) + "(" + str(ydata1[ind]) + "%)" ,ha='center', va='bottom')
		ind += 1 

	plt.savefig("LICENSE")

	vsql_args = ["vsql" ,"-h", args.host, "-U","dbadmin","-w",args.password,"-HXc" ]
        p=Popen(vsql_args + [sql] ,stdout=PIPE)
        (html,err) = p.communicate()
        result.append(html)

	html = """<img src="cid:STUDIO"><BR> """ + result[0] + """
                  <img src="cid:PIE_STUDIO"><BR>
		  <img src="cid:STUDIO_HISTORY"><BR> """ + result[1] + """
                  <img src="cid:SCHEMA"><BR> """ + result[2] + """
                  <img src="cid:LICENSE"><BR>""" + result[3]

	# Record the MIME types.
	msgHtml = MIMEText(html, 'html')
	msg.attach(msgHtml)
	msg['Subject'] = "DB License Monthly Charts(EST TZ)- " + str(args.host)+ "-" + str(args.type) 

	for i in ['STUDIO','STUDIO_HISTORY','SCHEMA','PIE_STUDIO','LICENSE']:
		img = open(i + '.png', 'rb').read()
  		msgImg = MIMEImage(img, 'png')
  		msgImg.add_header('Content-ID', '<'+i+'>')
 		msgImg.add_header('Content-Disposition', 'inline', filename='"+i+".png')
  		msg.attach(msgImg)
	

	


def autolabel(ax, rects):
	for rect in rects:
                #ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,'%d' % int(height),ha='center', va='bottom',rotation=45)
		xloc = rect.get_width()
		yloc = rect.get_y() + rect.get_height()/2.0
        	ax.text( xloc + 10  , yloc, '%s' % '{0:,}'.format(int(xloc))  , ha='left',va='center')


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
                    help='report type to send MEM|MEMLARGE|LABEL|SPILL|GCL|OBJLOCK|RESWAIT|ALL|LICENSE|WAIT|BUCKET')
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

if args.type <> 'LICENSE' :
  html = """\
        <p>
		<img src="cid:mem"><BR>
                <img src="cid:gcl"><BR>
                <img src="cid:spill"><BR>
		<img src="cid:label"><BR>
		<img src="cid:reswait"><BR>
		<img src="cid:wait"><BR>
		<img src="cid:objlock"><BR>
		<img src="cid:bucket"><BR>
        </p>"""
  # Record the MIME types.
  msgHtml = MIMEText(html, 'html')
  msg.attach(msgHtml)

if args.type in ['MEMLARGE','ALL']:
       exec_memlarge2(msg)
if args.type in ['MEM','ALL']: # resource pool usage over time ( # queries, reserved_memory)
       exec_memusage(msg)
#if args.type in ['LABEL','ALL']: #labeled queries execution time + memory , last 7 days
#       exec_label(msg)
if args.type in ['SPILL','ALL']:
       exec_spilled(msg)
if args.type in ['GCL','ALL']:
       exec_gcl(msg)
if args.type in ['OBJLOCK','ALL']:
       exec_objlock (msg)
if args.type in ['RESWAIT','ALL']:
       exec_reswait(msg)
if args.type in ['LICENSE']:
       ret = exec_license(msg)
if args.type in ['WAIT','ALL']:
       exec_wait(msg)
if args.type in ['BUCKET','ALL']:
       exec_bucket(msg)


db.close()

if args.email is not None:
        # Send the message via our own SMTP server, but don't include the envelope header.
        s = smtplib.SMTP('localhost')
        s.sendmail(me,[you],msg.as_string())
        s.quit()
