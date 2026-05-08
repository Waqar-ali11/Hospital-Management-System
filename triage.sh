#!/bin/bash

# ============================================================
# Project : Hospital Patient Triage & Bed Allocator
# Script : triage .sh
# Group : Group XX
# Members : Waqar Ali (24F -0800) , Muhammad Zulqarnain (24F -0536)
# Date : 2026 -05 -03
# Purpose : Compute triage priority and pipe patient data
# to the admissions manager process .
# Usage : ./ triage .sh <name > <age > <severity 1 -10 >
# ============================================================

patient_name=$1    # The data will automatically declare to these variables
patient_age=$2     # afer running ./triage.sh name age severity
patient_severity=$3

# condition to check that variables are not empty
if [ -z "$patient_name" ] || [ -z "$patient_age" ] || [ -z "$patient_severity" ]; then
	echo "You have not provided complete arguments."
	echo "Please write properly: triage.sh name age severity"
	exit 1
fi

# condition to check that age is a valid numeric number, not a string
if ! [[ "$patient_age" =~ ^[0-9]+$ ]]; then
	echo "You have entered incorrect age."
	exit 1
fi

# condition to check that patient's severity must between 1-10
if ! [[ "$patient_severity" =~ ^[0-9]+$ ]] || [ "$patient_severity" -lt 1 ] || [ "$patient_severity" -gt 10 ]; then
	echo "Patient severity is not a valid number between 1 and 10."
	exit 1
fi

# conditions to assign priority based on patient's severity
if [ "$patient_severity" -ge 9 ]; then
	triage_priority=1
elif [ "$patient_severity" -ge 7 ]; then
	triage_priority=2
elif [ "$patient_severity" -ge 5 ]; then
	triage_priority=3
elif [ "$patient_severity" -ge 3 ]; then
	triage_priority=4
else
	triage_priority=5
fi

patient_record="$patient_name, $patient_age, $patient_severity, $triage_priority"
echo "Triage Complete = $patient_record"

if [ -p /tmp/triage_fifo ]; then     # changes for phase 2 : Pipe it directly to the Admissions Process
    echo "Triage Complete = $patient_record" > /tmp/triage_fifo
fi
