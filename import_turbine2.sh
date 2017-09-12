#!/bin/bash

pass=`tail -1 pass_file | cut -d' ' -f3`
myvsql="vsql -h turbine -w ${pass} -XtAc"
#table="wbnet.AMS_dbo_Accounts"
#table="wbnet_history.IRS_fabrika_PaymentNotification"
#table="wbnet.IRS_billing_Transaction"
table="ads.WBID_owner_WBID_activity_ranked"
echo $table
	
	IFS='.' read -r -a arr <<< "$table"

	echo " Starting import on $table "
	date

	 aws s3 cp s3://gaia-vertica-backup/export_turbine/${arr[0]}/${table}.gz - | $myvsql "COPY ${table}_restore FROM STDIN GZIP DELIMITER '' NULL '\N*' DIRECT NO ESCAPE" 

	echo " $table imported"
	date

