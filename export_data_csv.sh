#!/bin/bash

#pass=`tail -1 pass_file | cut -d' ' -f3`
pass=3SIj9ShC032614S
myvsql="vsql -h dw.monolith.insights.dgs.io -U dbadmin -w ${pass}  -F  -P null=\N*  -XtAc"
i=0

#establish what tables to export 

for table in `$myvsql "select projection_schema || '.' || anchor_table_name  from projection_storage 
                        where (regexp_like(anchor_table_name,'^profile_') is true OR regexp_like(anchor_table_name,'^npc_Npc') is true )  
                        and projection_schema ='firebird_production_history'
                        group by 1 
                        having sum(used_bytes) > 0 
                        order by sum(used_bytes) ASC limit 5  " `
do
	((i++)) # run 5 jobs in parallel then wait, a better alternative is to run 5 workers with workers getting a new job after their current is done 
		# to rewrite in python 

	# $table is of format schema.tablename , need to tokenize it 
	IFS='.' read -r -a arr <<< "$table"

	echo " Starting export on $table "

	for partition in `$myvsql "select date(gaia_create_dt) from ${table} group by 1 order by 1 "`
	do 
		echo $part


	 	(
		#$myvsql "select *  from ${table};" | lzop | aws s3 cp - s3://gaia-vertica-backup/export_SOM_test/${arr[0]}/${table}_${part}.gz
		echo " $table exported ."
		sleep 10
		date  ) &

	done

	if (( $i % 5 == 0 ))  
	then 
	      echo "waiting..."
	      wait
	fi

done 

