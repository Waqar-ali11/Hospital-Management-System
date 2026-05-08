#include <stdio.h>
#include <stdlib.h>	// for memory allocation
#include <string.h>
#include <time.h>
#include <errno.h>	// for system error reporting codes
#include <unistd.h>	// for process creation (fork, execv)
#include <signal.h>	// for signal handling (sigaction, signal, SIGCHLD, SIGTERM)
#include <sys/wait.h>
#include <sys/ipc.h>	// for System V IPC key generation (ftok) used to identify the shared memory segment
#include <sys/shm.h>	// for System V Shared Memory operations (shmget, shmat, shmdt, shmctl) to manage the bed bitmap
#include <sys/types.h>	// defines system data types used in IPC and processes (e.g., pid_t, key_t)
#include <sys/stat.h>	// for file status and creating named pipes/FIFOs via mkfifo()
#include <fcntl.h>	// for file control options (O_RDWR, O_NONBLOCK) used to open the FIFOs without blocking

#define MAX_BEDS 10
#define SHM_PATH "/tmp/hospital_shared_memory"

// Supported Scheduling Algorithms
typedef enum {
    FCFS,
    PRIORITY
} SchedulingAlgo;

SchedulingAlgo current_algo = PRIORITY; // Default

// Shared memory structure for bed bitmap
typedef struct {
    int bed_bitmap[MAX_BEDS];	// 0 = free, 1 = occupied
} SharedData;	// Mapping: 0-3 (ICU), 4-7 (Isolation), 8-9 (General)

// Patient Queue Node
typedef struct Patient {
    int id;
    char name[50];
    int age;
    int severity;
    int priority;
    time_t arrival_time;
    int burst_time;
    struct Patient* next;
} Patient;

volatile sig_atomic_t keep_running = 1;
Patient* queue_head = NULL;
int shmid;
SharedData* shm_ptr = NULL;
int patient_counter = 1;
FILE* log_file;

int total_wait_time = 0;
int total_turnaround_time = 0;
int completed_patients = 0;

// Signal Handlers
void sigchld_handler(int sig) {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        // Zombie reaped silently
    }
}

void sigterm_handler(int sig) {
    keep_running = 0;
}

// Universal Enqueue logic based on selected scheduling algorithm
void enqueue_patient(Patient* new_patient) {
    if (!queue_head) {
        queue_head = new_patient;
        return;
    }

    if (current_algo == FCFS) {
        Patient* current = queue_head;	// First-Come First-Served
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = new_patient;

    } else if (current_algo == PRIORITY) {
        if (queue_head->priority > new_patient->priority) {	// Priority Scheduling, Sort ascending by triage level (1 is highest)
            new_patient->next = queue_head;
            queue_head = new_patient;
        } else {
            Patient* current = queue_head;
            while (current->next != NULL && current->next->priority <= new_patient->priority) {
                current = current->next;
            }
            new_patient->next = current->next;
            current->next = new_patient;
        }
    }
}

// Queue Dequeue
Patient* dequeue() {
    if (!queue_head) return NULL;
    Patient* p = queue_head;
    queue_head = queue_head->next;
    return p;
}

int generate_burst_time(int priority) {
    if (priority <= 2) return (rand() % 11) + 5;	// ICU: 5-15s, Isolation: 3-10s, General: 2-8s
    if (priority <= 4) return (rand() % 8) + 3;
    return (rand() % 7) + 2;
}

int find_free_bed(int priority) {
    int start_idx = 0, end_idx = MAX_BEDS - 1;
    if (priority <= 2) { start_idx = 0; end_idx = 3; }
    else if (priority <= 4) { start_idx = 4; end_idx = 7; }
    else { start_idx = 8; end_idx = 9; }

    for (int i = start_idx; i <= end_idx; i++) {
        if (shm_ptr->bed_bitmap[i] == 0) return i;
    }
    return -1;
}

int main(int argc, char* argv[]) {
    srand(time(NULL));
    char algo_name[20] = "PRIORITY";	// Parse Command Line Argument for Scheduling Algorithm
    if (argc > 1) {
        if (strcmp(argv[1], "FCFS") == 0) {
            current_algo = FCFS;
            strcpy(algo_name, "FCFS");
        } else if (strcmp(argv[1], "PRIORITY") == 0) {
            current_algo = PRIORITY;
            strcpy(algo_name, "PRIORITY");
        } else {
            printf("Invalid algorithm. Use FCFS or PRIORITY. Defaulting to PRIORITY.\n");
        }
    }
    struct sigaction sa_chld;	// Setup Signal Handlers
    sa_chld.sa_handler = sigchld_handler;
    sigemptyset(&sa_chld.sa_mask);
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);
    signal(SIGTERM, sigterm_handler);
    signal(SIGINT, sigterm_handler);
    key_t key = ftok(SHM_PATH, 65);	// Setup Shared Memory
    shmid = shmget(key, sizeof(SharedData), 0666 | IPC_CREAT);
    if (shmid == -1) { perror("shmget failed"); exit(1); }
    shm_ptr = (SharedData*) shmat(shmid, NULL, 0);
    memset(shm_ptr->bed_bitmap, 0, sizeof(shm_ptr->bed_bitmap));

    mkfifo("/tmp/triage_fifo", 0666);	// Setup FIFOs
    mkfifo("/tmp/discharge_fifo", 0666);

    int triage_fd = open("/tmp/triage_fifo", O_RDWR | O_NONBLOCK);
    int discharge_fd = open("/tmp/discharge_fifo", O_RDWR | O_NONBLOCK);

    log_file = fopen("schedule_log.txt", "w");
    fprintf(log_file, "--- Scheduling Log (Algorithm: %s) ---\n", algo_name);
    fflush(log_file);

    char t_buf[256];
    char d_buf[64];

    while (keep_running) {
        memset(t_buf, 0, sizeof(t_buf));	// 1. Read from Triage FIFO
        if (read(triage_fd, t_buf, sizeof(t_buf)-1) > 0) {
            Patient* p = malloc(sizeof(Patient));
            p->id = patient_counter++;
            p->arrival_time = time(NULL);

            sscanf(t_buf, "Triage Complete = %[^,], %d, %d, %d",
                   p->name, &p->age, &p->severity, &p->priority);

            p->burst_time = generate_burst_time(p->priority);
            enqueue_patient(p);
        }
        memset(d_buf, 0, sizeof(d_buf));	// 2. Read from Discharge FIFO
        if (read(discharge_fd, d_buf, sizeof(d_buf)-1) > 0) {
            int pid_discharged, bed_freed;
            sscanf(d_buf, "%d,%d", &pid_discharged, &bed_freed);
            shm_ptr->bed_bitmap[bed_freed] = 0;
            completed_patients++;
        }
        if (queue_head != NULL) {	// 3. Scheduler Logic
            int target_bed = find_free_bed(queue_head->priority);

            if (target_bed != -1) {
                Patient* p = dequeue();
                shm_ptr->bed_bitmap[target_bed] = 1;
                int wait_time = time(NULL) - p->arrival_time;
                int turnaround_time = wait_time + p->burst_time;
                total_wait_time += wait_time;
                total_turnaround_time += turnaround_time;

                fprintf(log_file, "[Admitted] Patient %d (Pri: %d, Burst: %ds). Wait: %ds, Turnaround: %ds, Bed: %d\n",
                        p->id, p->priority, p->burst_time, wait_time, turnaround_time, target_bed);
                fflush(log_file);

                pid_t child_pid = fork();
                if (child_pid == 0) {
                    char id_str[10], prio_str[10], bed_str[10], burst_str[10];
                    sprintf(id_str, "%d", p->id);
                    sprintf(prio_str, "%d", p->priority);
                    sprintf(bed_str, "%d", target_bed);
                    sprintf(burst_str, "%d", p->burst_time);

                    char* args[] = {"./patient_simulator", id_str, prio_str, bed_str, burst_str, NULL};
                    execv("./patient_simulator", args);
                    perror("execv failed");
                    exit(1);
                }
                free(p);
            }
        }
        usleep(100000);
    }

    if (completed_patients > 0) {
        fprintf(log_file, "\n--- Simulation Summary ---\n");
        fprintf(log_file, "Algorithm Used: %s\n", algo_name);
        fprintf(log_file, "Total Patients Completed: %d\n", completed_patients);
        fprintf(log_file, "Average Waiting Time: %.2f seconds\n", (float)total_wait_time / completed_patients);
        fprintf(log_file, "Average Turnaround Time: %.2f seconds\n", (float)total_turnaround_time / completed_patients);
    }

    fclose(log_file);
    close(triage_fd);
    close(discharge_fd);
    shmdt(shm_ptr);
    shmctl(shmid, IPC_RMID, NULL);

    return 0;
}
