# Hospital Patient Triage & Bed Allocator

This project is a multi-threaded Operating Systems simulation of a hospital admissions manager. It demonstrates core OS concepts including process management, inter-process communication (IPC), thread synchronization, CPU scheduling, and dynamic memory management.

## Features
* **Process Management:** Uses `fork()` and `execv()` to spawn concurrent patient processes.
* **IPC:** Uses Named Pipes (FIFOs) and System V Shared Memory to communicate between shell scripts and C programs.
* **Thread Synchronization:** Uses POSIX threads, Mutexes, and Condition Variables to manage a thread pool (Receptionist, Scheduler, Nurses).
* **Semaphores:** Enforces ward capacity limits using bounded counting semaphores.
* **Scheduling Algorithms:** Supports both **Priority** and **First-Come-First-Served (FCFS)** scheduling queues.
* **Dynamic Memory Allocator:** Treats the hospital ward as a contiguous memory block with **Best-Fit**, **First-Fit**, and **Worst-Fit** allocation, including memory coalescing and fragmentation reporting.
* **Virtual Memory:** Uses `mmap()` to log patient records directly to a memory-mapped file.

## Requirements
To compile and run this project, you need a Linux environment (like Ubuntu) with the following tools installed:
* GCC (GNU Compiler Collection)
* Make
* Bash shell

## Setup & Installation

**1. Clone the repository and navigate to the project folder:**
```bash
git clone <your-github-repo-url>
cd <your-folder-name>
2. Make the shell scripts executable:


chmod +x start_hospital.sh stop_hospital.sh triage.sh
3. Compile the C programs:

make clean
make all
This will create two executable files: admissions and patient_simulator.

How to Run the Simulation
Step 1: Start the Hospital
Launch the admissions manager in the background. By default, it uses Priority scheduling and Best-Fit memory allocation.

./start_hospital.sh
Optional: You can pass a memory strategy argument (--strategy=first, --strategy=best, or --strategy=worst):

./start_hospital.sh --strategy=first
Step 2: Admit Patients
Use the triage script to send patients to the hospital. The format is ./triage.sh "<Name>" <Age> <Severity 1-10>.

./triage.sh "Waqar" 19 2
./triage.sh "Zulqarnain" 24 9
./triage.sh "Ali" 22 5
Step 3: Stop the Hospital
When you are done simulating, gracefully shut down the hospital. This script will safely terminate processes, clean up shared memory, and destroy the IPC pipes.

./stop_hospital.sh
Logs & Outputs
After running and stopping the simulation, the system generates three important log files:

schedule_log.txt - Shows patient waiting times, turnaround times, and the scheduling summary.

memory_log.txt - Shows free memory units, largest block size, and fragmentation statistics.

patient_records.dat - A binary/text file mapped directly to virtual memory using mmap(), keeping a persistent record of admissions and discharges.

Group Members
Waqar Ali (24F-0800)

Muhammad Zulqarnain (24F-0536)
