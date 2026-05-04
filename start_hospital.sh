#!/bin/bash

# condition to make discharge_fifo if not exists
if [ ! -p /tmp/discharge_fifo ]; then
	mkfifo /tmp/discharge_fifo
	echo "Named Pipe discharge fifo is created."
fi

# key file for hospital shared memory
touch /tmp/hospital_shared_memory
echo "Shared memory file is initialized"

# remove any previous semaphores by using -f forcefully to make sure that the
# errors not occurs during removing
rm -f /dev/shm/sem.*
echo "Semaphore's environment also initialized."

echo "------------------------------------------"
echo "   STARTING HOSPITAL ADMISSIONS MANAGER"
echo "    WARDS CAPACITY = 10 BEDS AVAILABLE"
echo "------------------------------------------"

# Run process in background & is used for this purpose
# $! is for giving the variable latest PID run on previous command
./admissions &
admissions_PID=$!

# printing the PID of process into text file for later use
echo $admissions_PID > admissions_PID.txt
echo "Admissions manager launched successfully in background"
