/*
 * ============================================================
 * Project : Hospital Patient Triage & Bed Allocator
 * File    : admissions.c
 * Group   : Group XX
 * Members : Waqar Ali (24F-0800), Muhammad Zulqarnain (24F-0536)
 * Date    : 2026-05-08
 * Purpose : Central admissions manager - process spawning,
 * IPC, thread pool, scheduling, and bed allocation.
 * Compile : gcc -Wall -o admissions admissions.c -pthread
 * ============================================================
 */

#include <stdio.h>
#include <stdlib.h>      // memory allocation and random numbers
#include <string.h>      // string manipulation
#include <time.h>
#include <errno.h>       // error reporting
#include <unistd.h>	 // process creation and sleep functions
#include <signal.h>      // signal handling for clean shutdown
#include <sys/wait.h>    // waiting for child processes
#include <sys/ipc.h>     // inter-process communication keys
#include <sys/shm.h>     // shared memory functions
#include <sys/types.h>   // system data types
#include <sys/stat.h>    // file status and named pipes
#include <fcntl.h>       // file control options
#include <pthread.h>
#include <semaphore.h>   // semaphores for capacity limits

#define MAX_BEDS 10
#define SHM_PATH "/tmp/hospital_shared_memory" // path for shared memory key
#define MAX_QUEUE_SIZE 50	// maximum patients allowed in queue

typedef enum {FCFS, PRIORITY} SchedulingAlgo;	// supported scheduling types
SchedulingAlgo current_algo = PRIORITY;         // defaults to priority scheduling

typedef struct {
    int bed_bitmap[MAX_BEDS]; // 0 means bed is free, 1 means that bed is occupied
} SharedData;

typedef struct Patient {
    int id;
    char name[50];
    int age;
    int severity;
    int priority;
    time_t arrival_time;
    int burst_time;
    struct Patient* next;	// pointer to next patient in queue
} Patient;

volatile sig_atomic_t keep_running = 1;	// keeps loops running until told to stop
int shmid;
SharedData* shm_ptr = NULL;
FILE* log_file;		// file for saving the schedule log
int patient_counter = 1;

int total_wait_time = 0;
int total_turnaround_time = 0;
int completed_patients = 0;
Patient* queue_head = NULL;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;	// locks the queue during updates
pthread_cond_t cond_queue_not_empty = PTHREAD_COND_INITIALIZER; 	// signals when a patient is added
sem_t sem_queue_limit;		// limits the queue size so it doesn't overflow

pthread_mutex_t bed_mutex = PTHREAD_MUTEX_INITIALIZER;		// locks the bed array during updates
pthread_cond_t cond_bed_freed = PTHREAD_COND_INITIALIZER;	// signals when a bed becomes available
int bed_needs_cleaning[MAX_BEDS] = {0};		// array to flag beds that need a nurse
pthread_cond_t cond_nurse_wakeup = PTHREAD_COND_INITIALIZER;	// wakes up nurses to clean beds

sem_t sem_icu;		// limits icu admissions to max 4
sem_t sem_iso;		// limits isolation admissions to max 4

typedef struct {
    int ward_type;	// 0 for icu, 1 for iso, 2 for general
    char ward_name[20];
    int start_bed;
    int end_bed;
    sem_t* capacity_sem;
} WardConfig;

void sigchld_handler(int sig) {
    int status;
    while (waitpid(-1, &status, WNOHANG) > 0);		// silently clean up dead child processes
}

void sigterm_handler(int sig) {
    keep_running = 0;		// switch flag to stop all while loops
    pthread_cond_broadcast(&cond_queue_not_empty); 	// wake up any sleeping scheduler threads
    pthread_cond_broadcast(&cond_bed_freed);		// wake up any threads waiting for beds
    pthread_cond_broadcast(&cond_nurse_wakeup);		// wake up sleeping nurses so they can exit
}

int generate_burst_time(int priority) {
    if (priority <= 2) return (rand() % 11) + 5;	// icu patients take 5 to 15 seconds
    if (priority <= 4) return (rand() % 8) + 3;		// isolation patients take 3 to 10 seconds
    return (rand() % 7) + 2;		// general patients take 2 to 8 seconds
}

int find_free_bed(int priority) {
    int start = 0, end = 9;
    if (priority <= 2) { start = 0; end = 3; }		// map high priority to icu beds
    else if (priority <= 4) { start = 4; end = 7; }
    else { start = 8; end = 9; }                    // map low priority to general beds

    for (int i = start; i <= end; i++) {            // loop through the assigned ward section
        if (shm_ptr->bed_bitmap[i] == 0) return i;
    }
    return -1;		// return -1 if ward is full
}

void* receptionist_thread(void* arg) {
    int triage_fd = open("/tmp/triage_fifo", O_RDWR | O_NONBLOCK);	// open pipe to read patients
    char t_buf[256];		// buffer for incoming text

    while (keep_running) {		// run until hospital stops
        memset(t_buf, 0, sizeof(t_buf));		// clear the buffer
        if (read(triage_fd, t_buf, sizeof(t_buf)-1) > 0) {	// check if data came through
            Patient* p = malloc(sizeof(Patient));	// allocate memory for new patient
            p->id = patient_counter++;
            p->arrival_time = time(NULL);		// save current time
            sscanf(t_buf, "Triage Complete = %[^,], %d, %d, %d",	// extract data from string
                   p->name, &p->age, &p->severity, &p->priority);
            p->burst_time = generate_burst_time(p->priority);		// calculate treatment time

            sem_wait(&sem_queue_limit);		// wait here if queue is full

            pthread_mutex_lock(&queue_mutex);
            if (!queue_head) {
                queue_head = p;
                p->next = NULL;
            } else if (current_algo == FCFS) {
                Patient* curr = queue_head;
                while (curr->next) curr = curr->next;
                curr->next = p;
                p->next = NULL;
            } else {		// if using priority scheduling
                if (queue_head->priority > p->priority) {
                    p->next = queue_head;
                    queue_head = p;
                } else {
                    Patient* curr = queue_head;
                    while (curr->next && curr->next->priority <= p->priority) curr = curr->next;
                    p->next = curr->next;
                    curr->next = p;
                }
            }
            pthread_cond_signal(&cond_queue_not_empty);		// notify scheduler there is a patient ready
            pthread_mutex_unlock(&queue_mutex);		// release the queue lock
        }
        usleep(100000);		// sleep a tiny bit to stop high cpu usage
    }
    close(triage_fd);		// close the pipe before exiting
    return NULL;
}

void* scheduler_thread(void* arg) {
    while (keep_running) {
        pthread_mutex_lock(&queue_mutex);
        while (queue_head == NULL && keep_running) {
            pthread_cond_wait(&cond_queue_not_empty, &queue_mutex);	// sleep until a patient arrives
        }
        if (!keep_running) { pthread_mutex_unlock(&queue_mutex); break; }	// exit if shutting down

        Patient* p = queue_head;
        queue_head = queue_head->next;
        pthread_mutex_unlock(&queue_mutex);
        sem_post(&sem_queue_limit);	// free up a slot in the receptionist's queue limit

        if (p->priority <= 2) sem_wait(&sem_icu);	// block if icu is full
        else if (p->priority <= 4) sem_wait(&sem_iso);	// block if isolation is full

        pthread_mutex_lock(&bed_mutex);
        int target_bed = -1;	// default to invalid bed
        while ((target_bed = find_free_bed(p->priority)) == -1 && keep_running) {
            pthread_cond_wait(&cond_bed_freed, &bed_mutex);	// wait until a nurse cleans a bed
        }
        if (!keep_running) { pthread_mutex_unlock(&bed_mutex); break; }		// handle shutdown properly

        shm_ptr->bed_bitmap[target_bed] = 1;
        pthread_mutex_unlock(&bed_mutex);

        int wait_time = time(NULL) - p->arrival_time;
        total_wait_time += wait_time;
        total_turnaround_time += (wait_time + p->burst_time);

        fprintf(log_file, "[Scheduler] Admitted Patient %d (Pri: %d). Wait: %ds, Bed: %d\n", 
                p->id, p->priority, wait_time, target_bed);	// log the event
        fflush(log_file);		// force write to file

        pid_t child_pid = fork();	// create new process for patient
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
        free(p);	// free memory in the parent process
    }
    return NULL;
}

void* discharge_listener_thread(void* arg) {
    int discharge_fd = open("/tmp/discharge_fifo", O_RDWR | O_NONBLOCK);	// open pipe to hear discharges
    char d_buf[64];		// buffer for messages

    while (keep_running) {
        memset(d_buf, 0, sizeof(d_buf));		// clear old messages
        if (read(discharge_fd, d_buf, sizeof(d_buf)-1) > 0) {		// check if patient left
            int pid_discharged, bed_freed;
            sscanf(d_buf, "%d,%d", &pid_discharged, &bed_freed);

            pthread_mutex_lock(&bed_mutex);
            bed_needs_cleaning[bed_freed] = 1;		// tell nurse this bed is dirty
            completed_patients++;
            pthread_cond_broadcast(&cond_nurse_wakeup);		// wake up all nurses to check beds
            pthread_mutex_unlock(&bed_mutex);		// unlock beds
        }
        usleep(100000);
    }
    close(discharge_fd);	// close pipe on exit
    return NULL;
}

void* nurse_thread(void* arg) {
    WardConfig* config = (WardConfig*)arg;	// unpack thread arguments

    while (keep_running) {
        pthread_mutex_lock(&bed_mutex);
        int dirty_bed = -1;		// default to no dirty bed
        while (keep_running) {
            for (int i = config->start_bed; i <= config->end_bed; i++) {
                if (bed_needs_cleaning[i]) {
                    dirty_bed = i;
                    break;
                }
            }
            if (dirty_bed != -1) break;
            pthread_cond_wait(&cond_nurse_wakeup, &bed_mutex);
        }
        if (!keep_running) { pthread_mutex_unlock(&bed_mutex); break; }		// handle shutdown properly

        bed_needs_cleaning[dirty_bed] = 0;		// mark bed as clean
        shm_ptr->bed_bitmap[dirty_bed] = 0;		// free it in shared memory

        printf("[Nurse - %s] Cleaned and freed bed %d. (Attempted Coalescing)\n", config->ward_name, dirty_bed);

        if (config->capacity_sem != NULL) {
            sem_post(config->capacity_sem);
        }

        pthread_cond_broadcast(&cond_bed_freed);		// announce that a bed is ready
        pthread_mutex_unlock(&bed_mutex);
    }
    free(config);
    return NULL;
}

int main(int argc, char* argv[]) {
    srand(time(NULL));

    if (argc > 1 && strcmp(argv[1], "FCFS") == 0) current_algo = FCFS;		// switch algorithm if requested

    struct sigaction sa_chld;		// struct for child signal
    sa_chld.sa_handler = sigchld_handler;
    sigemptyset(&sa_chld.sa_mask);		// clear flags
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;	// set flags for clean reaping
    sigaction(SIGCHLD, &sa_chld, NULL);		// catch zombie processes
    signal(SIGTERM, sigterm_handler);		// catch kill signals
    signal(SIGINT, sigterm_handler);		// catch ctrl+c

    key_t key = ftok(SHM_PATH, 65);		// generate memory key
    shmid = shmget(key, sizeof(SharedData), 0666 | IPC_CREAT);		// request shared block
    shm_ptr = (SharedData*) shmat(shmid, NULL, 0);	// attach it to our space
    memset(shm_ptr->bed_bitmap, 0, sizeof(shm_ptr->bed_bitmap));	// set all beds to free

    sem_init(&sem_icu, 0, 4);		// set icu limit to 4
    sem_init(&sem_iso, 0, 4);		// set isolation limit to 4
    sem_init(&sem_queue_limit, 0, MAX_QUEUE_SIZE);		// set queue limit to 50

    mkfifo("/tmp/triage_fifo", 0666);		// create triage pipe
    mkfifo("/tmp/discharge_fifo", 0666);	// create discharge pipe

    log_file = fopen("schedule_log.txt", "w");		// open log file
    fprintf(log_file, "--- Scheduling Log (Algo: %s | Mode: Multithreaded) ---\n", 
            current_algo == FCFS ? "FCFS" : "PRIORITY");		// write header

    pthread_t t_receptionist, t_scheduler, t_discharge;
    pthread_create(&t_receptionist, NULL, receptionist_thread, NULL);
    pthread_create(&t_scheduler, NULL, scheduler_thread, NULL);
    pthread_create(&t_discharge, NULL, discharge_listener_thread, NULL);

    pthread_t t_nurses[3];		// array for nurse threads
    WardConfig* icu_config = malloc(sizeof(WardConfig));
    icu_config->ward_type = 0; strcpy(icu_config->ward_name, "ICU");
    icu_config->start_bed = 0; icu_config->end_bed = 3; icu_config->capacity_sem = &sem_icu;
    pthread_create(&t_nurses[0], NULL, nurse_thread, icu_config);

    WardConfig* iso_config = malloc(sizeof(WardConfig));
    iso_config->ward_type = 1; strcpy(iso_config->ward_name, "Isolation");
    iso_config->start_bed = 4; iso_config->end_bed = 7; iso_config->capacity_sem = &sem_iso;
    pthread_create(&t_nurses[1], NULL, nurse_thread, iso_config);

    WardConfig* gen_config = malloc(sizeof(WardConfig));	// setup general config
    gen_config->ward_type = 2; strcpy(gen_config->ward_name, "General");
    gen_config->start_bed = 8; gen_config->end_bed = 9; gen_config->capacity_sem = NULL;
    pthread_create(&t_nurses[2], NULL, nurse_thread, gen_config);

    while (keep_running) { pause(); }		// main thread sleeps until terminated

    pthread_join(t_receptionist, NULL);
    pthread_join(t_scheduler, NULL);
    pthread_join(t_discharge, NULL);
    for (int i = 0; i < 3; i++) pthread_join(t_nurses[i], NULL);

    if (completed_patients > 0) {
        fprintf(log_file, "\n--- Simulation Summary ---\n");
        fprintf(log_file, "Total Patients Completed: %d\n", completed_patients);
        fprintf(log_file, "Average Wait Time: %.2fs\n", (float)total_wait_time / completed_patients);
        fprintf(log_file, "Avg Turnaround Time: %.2fs\n", (float)total_turnaround_time / completed_patients);
    }

    fclose(log_file);		// close log file
    sem_destroy(&sem_icu);
    sem_destroy(&sem_iso);
    sem_destroy(&sem_queue_limit);
    shmdt(shm_ptr);		// detach shared memory
    shmctl(shmid, IPC_RMID, NULL);	// delete shared memory
    return 0;
}
