#!/bin/bash

# if condition to terminate the admission manager process
if [ -f admissions_PID.txt ]; then
	admissions_PID=$(cat admissions_PID.txt)
	echo "Sending SIGTERM to admission process"
	echo "PID : $admissions_PID"
	kill -15 $admissions_PID
	rm admissions_PID.txt
else
	echo "Process ID file not found. So, now we will remove it by name"
	pkill -15 admissions
fi

# give process a second to shut down by itself
sleep 1
echo "Cleaning up shared memory, FIFOs and named pipes..."

if [ -p /tmp/discharge_fifo ]; then
	rm /tmp/discharge_fifo
fi

if [ -f /tmp/hospital_shared_memory ]; then
	rm /tmp/hospital_shared_memory
fi

# this finds any shared memory owned by you and removes them
ipcs -m | grep $USER | awk '{print $2}' | xargs -r ipcrm -m

# now cleaning yp POSIX semaphores
rm -f /dev/shm/sem.*
