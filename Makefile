CC = gcc
CFLAGS = -Wall -pthread
all: admissions patient_simulator

admissions: admissions.c
	$(CC) $(CFLAGS) -o admissions admissions.c

patient_simulator: patient_simulator.c
	$(CC) $(CFLAGS) -o patient_simulator patient_simulator.c

clean:
	rm -f admissions patient_simulator admissions_PID.txt

run: all
	./start_hospital.sh

test: all
	./triage.sh "Waqar Ali" 19 9
