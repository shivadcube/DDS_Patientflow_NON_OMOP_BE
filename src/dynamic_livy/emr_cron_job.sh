#!/bin/bash

while [ true ]; do
 currentDate=`date`
 echo $currentDate
 sleep 10
 python emr_lot_job_status.py
done
