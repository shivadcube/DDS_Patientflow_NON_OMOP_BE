#!/bin/bash

while [ true ]; do
 currentDate=`date`
 echo $currentDate
 sleep 10
 python lot_job_status.py
done
