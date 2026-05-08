/*
 * ============================================================
 * Project : Hospital Patient Triage & Bed Allocator
 * File    : admissions.c
 * Group   : Group XX
 * Members : Waqar Ali (24F-0800), Muhammad Zulqarnain (24F-0536)
 * Date    : 2026-05-07
 * Purpose : Central admissions manager - IPC, threads, scheduling,
 * and Dynamic Memory Management (Best/First/Worst Fit).
 * Compile : gcc -Wall -o admissions admissions.c -pthread
 * ============================================================
 */

#include <stdio.h>
#include <stdlib.h>	// memory allocation
#include <string.h>
#include <time.h>
#include <errno.h>	// error codes
#include <unistd.h>	// process management
#include <signal.h>		// signal handling
#include <sys/wait.h>	// waiting for children
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>	// data types
#include <sys/stat.h>
#include <sys/mman.h>		// mmap for virtual memory
#include <fcntl.h>	// file control
#include <pthread.h>
#include <semaphore.h>

#define WARD_SIZE 40
#define PAGE_SIZE 2		// fixed page size for paging simulation
#define SHM_PATH "/tmp/hospital_shared_memory"		// shared memory path
#define MAX_QUEUE_SIZE 50	// maximum queue limit
#define MMAP_FILE "patient_records.dat"		// virtual memory mapped file
#define MMAP_SIZE 4096		// mapped region size

typedef enum { FCFS, PRIORITY } SchedulingAlgo;
SchedulingAlgo current_algo = PRIORITY;		// defaults to priority scheduling

typedef enum { FIRST_FIT, BEST_FIT, WORST_FIT } AllocStrategy;
AllocStrategy current_strategy = BEST_FIT;	// defaults to best-fit memory allocation

typedef struct {
    int ward_memory[WARD_SIZE];		// 0 means free, and greater than 0 means occupied by patient id
} SharedData;

typedef struct FreeBlock {
    int start;
    int size;
    struct FreeBlock* next;		// pointer to next free block
} FreeBlock;

typedef struct Patient {
    int id;
    char name[50];
    int age;
    int severity;
    int priority;
    int care_units;		// memory units required for treatment
    time_t arrival_time;
    int burst_time;
    struct Patient* next;	// pointer for the queue linked list
} Patient;

volatile sig_atomic_t keep_running = 1;		// keeps threads running until shutdown
int shmid;
SharedData* shm_ptr = NULL;
FILE* schedule_log;
FILE* mem_log;

char* mmap_region = NULL;	// pointer for virtual memory
int mmap_offset = 0;		// tracks where to write next in mmap
pthread_mutex_t mmap_mutex = PTHREAD_MUTEX_INITIALIZER;

int patient_counter = 1;
int completed_patients = 0;

Patient* queue_head = NULL;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;	// locks patient queue
pthread_cond_t cond_queue_not_empty = PTHREAD_COND_INITIALIZER;		// signals new arrivals
sem_t sem_queue_limit;

FreeBlock* free_list_head = NULL;
pthread_mutex_t mem_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_mem_freed = PTHREAD_COND_INITIALIZER;	// signals when memory is freed

int bed_needs_cleaning[WARD_SIZE] = {0};		// flags memory blocks for cleaning
pthread_cond_t cond_nurse_wakeup = PTHREAD_COND_INITIALIZER;		// wakes up nurse to clean

sem_t sem_icu;		// capacity limits for severe patients
sem_t sem_iso;

void sigchld_handler(int sig) {
    int status;
    while (waitpid(-1, &status, WNOHANG) > 0);		// reap dead child processes silently
}

void sigterm_handler(int sig) {
    keep_running = 0;		// trigger shutdown sequence
    pthread_cond_broadcast(&cond_queue_not_empty);
    pthread_cond_broadcast(&cond_mem_freed);
    pthread_cond_broadcast(&cond_nurse_wakeup);
}

void init_free_list() {
    free_list_head = malloc(sizeof(FreeBlock));		// create initial master memory block
    free_list_head->start = 0;
    free_list_head->size = WARD_SIZE;
    free_list_head->next = NULL;
}

void print_ward_map(const char* label) {
    printf("%s: [ ", label);
    for (int i = 0; i < WARD_SIZE; i++) {	// print current state of memory array
        if (shm_ptr->ward_memory[i] == 0) printf("- ");
        else printf("%d ", shm_ptr->ward_memory[i]);
    }
    printf("]\n");
}

void report_memory_stats(int patient_units) {
    int total_free = 0, largest_block = 0;
    FreeBlock* curr = free_list_head;
    while (curr) {	// scan free list to compute stats
        total_free += curr->size;
        if (curr->size > largest_block) largest_block = curr->size;
        curr = curr->next;
    }

    float ext_frag = total_free > 0 ? (1.0 - ((float)largest_block / total_free)) * 100.0 : 0.0;	// calculate external fragmentation
    int pages_needed = (patient_units + PAGE_SIZE - 1) / PAGE_SIZE;	// ceiling division for paging
    int allocated_page_space = pages_needed * PAGE_SIZE;
    int internal_frag = allocated_page_space - patient_units;	// calculate internal fragmentation

    fprintf(mem_log, "[Time: %ld] Free Units: %d | Largest Block: %d | Ext Frag: %.2f%% | Int Frag (Paging): %d units\n",
            time(NULL), total_free, largest_block, ext_frag, internal_frag);
    fflush(mem_log);
}

int allocate_memory(int units_needed) {
    FreeBlock* prev = NULL;
    FreeBlock* curr = free_list_head;
    FreeBlock* selected_prev = NULL;
    FreeBlock* selected = NULL;

    while (curr) {		// scan free list based on chosen strategy
        if (curr->size >= units_needed) {
            if (current_strategy == FIRST_FIT) {
                selected = curr; selected_prev = prev; break;		// stop at first fit
            } else if (current_strategy == BEST_FIT) {
                if (!selected || curr->size < selected->size) {		// find smallest fit
                    selected = curr; selected_prev = prev;
                }
            } else if (current_strategy == WORST_FIT) {
                if (!selected || curr->size > selected->size) {	// find largest fit
                    selected = curr; selected_prev = prev;
                }
            }
        }
        prev = curr;
        curr = curr->next;
    }

    if (!selected) return -1;		// allocation failed, must wait for discharge

    int start_idx = selected->start;
    if (selected->size == units_needed) {		// exact match, remove node from free list
        if (selected_prev) selected_prev->next = selected->next;
        else free_list_head = selected->next;
        free(selected);
    } else {		// split partition and update node size
        selected->start += units_needed;
        selected->size -= units_needed;
    }
    return start_idx;
}

void free_memory_and_coalesce(int start_idx, int size) {
    FreeBlock* new_block = malloc(sizeof(FreeBlock));		// create freed block
    new_block->start = start_idx;
    new_block->size = size;
    new_block->next = NULL;

    if (!free_list_head) { free_list_head = new_block; return; }

    FreeBlock* curr = free_list_head;
    FreeBlock* prev = NULL;
    while (curr && curr->start < new_block->start) {		// insert in sorted order by start index
        prev = curr;
        curr = curr->next;
    }

    if (prev) { prev->next = new_block; new_block->next = curr; }
    else { new_block->next = free_list_head; free_list_head = new_block; }

    curr = free_list_head;
    while (curr && curr->next) {		// coalesce adjacent free blocks
        if (curr->start + curr->size == curr->next->start) { // check if blocks touch
            FreeBlock* temp = curr->next;
            curr->size += temp->size;	// merge sizes
            curr->next = temp->next;
            free(temp);
        } else {
            curr = curr->next;
        }
    }
}

void write_mmap_log(const char* record) {
    pthread_mutex_lock(&mmap_mutex);
    if (mmap_offset + strlen(record) < MMAP_SIZE) {
        sprintf(mmap_region + mmap_offset, "%s\n", record);	// write directly to virtual memory
        mmap_offset += strlen(record);
    }
    pthread_mutex_unlock(&mmap_mutex);
}

void* receptionist_thread(void* arg) {
    int triage_fd = open("/tmp/triage_fifo", O_RDWR | O_NONBLOCK);
    char t_buf[256];

    while (keep_running) {
        memset(t_buf, 0, sizeof(t_buf));
        if (read(triage_fd, t_buf, sizeof(t_buf)-1) > 0) {	// check for new patients
            Patient* p = malloc(sizeof(Patient));
            p->id = patient_counter++;
            p->arrival_time = time(NULL);
            sscanf(t_buf, "Triage Complete = %[^,], %d, %d, %d",
                   p->name, &p->age, &p->severity, &p->priority);
            p->burst_time = (rand() % 8) + 3;	// assign random treatment time
            p->care_units = (p->priority <= 2) ? 4 : (p->priority <= 4) ? 3 : 2;	// severe patients need more memory

            sem_wait(&sem_queue_limit);		// block if queue is full

            pthread_mutex_lock(&queue_mutex);
            if (!queue_head) { queue_head = p; p->next = NULL; }
            else if (current_algo == FCFS) {	// insert at end for fcfs
                Patient* curr = queue_head;
                while (curr->next) curr = curr->next;
                curr->next = p; p->next = NULL;
            } else {		// insert based on triage level for priority
                if (queue_head->priority > p->priority) { p->next = queue_head; queue_head = p; }
                else {
                    Patient* curr = queue_head;
                    while (curr->next && curr->next->priority <= p->priority) curr = curr->next;
                    p->next = curr->next; curr->next = p;
                }
            }
            pthread_cond_signal(&cond_queue_not_empty);		// alert scheduler
            pthread_mutex_unlock(&queue_mutex);
        }
        usleep(100000);		// prevent high cpu usage
    }
    close(triage_fd); return NULL;
}

void* scheduler_thread(void* arg) {
    while (keep_running) {
        pthread_mutex_lock(&queue_mutex);
        while (queue_head == NULL && keep_running) pthread_cond_wait(&cond_queue_not_empty, &queue_mutex);
        if (!keep_running) { pthread_mutex_unlock(&queue_mutex); break; }

        Patient* p = queue_head;		// dequeue highest priority patient
        queue_head = queue_head->next;
        pthread_mutex_unlock(&queue_mutex);
        sem_post(&sem_queue_limit);	// free up queue space

        if (p->priority <= 2) sem_wait(&sem_icu);	// enforce ward limits
        else if (p->priority <= 4) sem_wait(&sem_iso);

        pthread_mutex_lock(&mem_mutex);
        int target_idx = -1;
        while ((target_idx = allocate_memory(p->care_units)) == -1 && keep_running) {
            pthread_cond_wait(&cond_mem_freed, &mem_mutex);		// sleep if no memory fits
        }
        if (!keep_running) { pthread_mutex_unlock(&mem_mutex); break; }

        for (int i = target_idx; i < target_idx + p->care_units; i++) shm_ptr->ward_memory[i] = p->id;	// assign memory units
        report_memory_stats(p->care_units);
        pthread_mutex_unlock(&mem_mutex);

        char log_str[128];
        sprintf(log_str, "Admitted Patient %d (Pri: %d). Required Units: %d. Start Index: %d", p->id, p->priority, p->care_units, target_idx);
        write_mmap_log(log_str);	// write to mmap file

        pid_t child_pid = fork();		// spawn patient process
        if (child_pid == 0) {
            char id_str[10], prio_str[10], idx_str[10], burst_str[10];
            sprintf(id_str, "%d", p->id); sprintf(prio_str, "%d", p->priority);
            sprintf(idx_str, "%d", target_idx); sprintf(burst_str, "%d", p->burst_time);
            char* args[] = {"./patient_simulator", id_str, prio_str, idx_str, burst_str, NULL};
            execv("./patient_simulator", args);
            exit(1);
        }
        free(p);
    }
    return NULL;
}

void* discharge_listener_thread(void* arg) {
    int discharge_fd = open("/tmp/discharge_fifo", O_RDWR | O_NONBLOCK);
    char d_buf[64];

    while (keep_running) {
        memset(d_buf, 0, sizeof(d_buf));
        if (read(discharge_fd, d_buf, sizeof(d_buf)-1) > 0) {	// check if process finished
            int pid_discharged, idx_freed;
            sscanf(d_buf, "%d,%d", &pid_discharged, &idx_freed);
            pthread_mutex_lock(&mem_mutex);
            bed_needs_cleaning[idx_freed] = pid_discharged;	// flag memory for nurse cleanup
            completed_patients++;
            pthread_cond_broadcast(&cond_nurse_wakeup);	// wake up nurse
            pthread_mutex_unlock(&mem_mutex);
        }
        usleep(100000);
    }
    close(discharge_fd); return NULL;
}

void* nurse_thread(void* arg) {
    while (keep_running) {
        pthread_mutex_lock(&mem_mutex);
        int dirty_idx = -1; int target_pid = -1;
        while (keep_running) {		// search for dirty memory
            for (int i = 0; i < WARD_SIZE; i++) {
                if (bed_needs_cleaning[i] > 0) {
                    dirty_idx = i; target_pid = bed_needs_cleaning[i]; break;
                }
            }
            if (dirty_idx != -1) break;
            pthread_cond_wait(&cond_nurse_wakeup, &mem_mutex);		// sleep until a patient leaves
        }
        if (!keep_running) { pthread_mutex_unlock(&mem_mutex); break; }

        printf("\n[Nurse] Discharging Patient %d...\n", target_pid);
        print_ward_map("Before Coalesce");

        int freed_units = 0;
        bed_needs_cleaning[dirty_idx] = 0;	// clear flag
        for (int i = dirty_idx; i < WARD_SIZE; i++) {	// free contiguous units
            if (shm_ptr->ward_memory[i] == target_pid) {
                shm_ptr->ward_memory[i] = 0; freed_units++;
            } else break;
        }

        free_memory_and_coalesce(dirty_idx, freed_units);	// calling for coalescing

        print_ward_map("After Coalesce ");
        report_memory_stats(0);		// log fragmentation upon deallocation

        char log_str[128];
        sprintf(log_str, "Discharged Patient %d. Freed %d units at Index %d", target_pid, freed_units, dirty_idx);
        write_mmap_log(log_str);

        sem_post(&sem_icu); sem_post(&sem_iso);		// safely release capacity limits

        pthread_cond_broadcast(&cond_mem_freed);    // alert scheduler that memory is free
        pthread_mutex_unlock(&mem_mutex);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    srand(time(NULL));

    if (argc > 1) {	// parse command line for allocation strategy
        if (strstr(argv[1], "first")) current_strategy = FIRST_FIT;
        else if (strstr(argv[1], "worst")) current_strategy = WORST_FIT;
        else current_strategy = BEST_FIT;
    }

    struct sigaction sa; sa.sa_handler = sigchld_handler; sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP; sigaction(SIGCHLD, &sa, NULL);	// setup zombie reaping
    signal(SIGTERM, sigterm_handler); signal(SIGINT, sigterm_handler);

    key_t key = ftok(SHM_PATH, 65);	// setup shared memory segment
    shmid = shmget(key, sizeof(SharedData), 0666 | IPC_CREAT);
    shm_ptr = (SharedData*) shmat(shmid, NULL, 0);
    memset(shm_ptr->ward_memory, 0, sizeof(shm_ptr->ward_memory));
    init_free_list();		// initialize dynamic memory allocator

    int mmap_fd = open(MMAP_FILE, O_RDWR | O_CREAT, 0666);	// setup mmap virtual memory
    ftruncate(mmap_fd, MMAP_SIZE); 
    mmap_region = mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, mmap_fd, 0);

    sem_init(&sem_icu, 0, 4); sem_init(&sem_iso, 0, 4); sem_init(&sem_queue_limit, 0, MAX_QUEUE_SIZE); 
    mkfifo("/tmp/triage_fifo", 0666); mkfifo("/tmp/discharge_fifo", 0666);	// setup posix fifos

    mem_log = fopen("memory_log.txt", "w");	// initialize fragmentation log
    fprintf(mem_log, "--- Memory Fragmentation Log (Strategy: %d) ---\n", current_strategy);

    pthread_t t_rec, t_sch, t_dis, t_nurse;	// declare system threads
    pthread_create(&t_rec, NULL, receptionist_thread, NULL);
    pthread_create(&t_sch, NULL, scheduler_thread, NULL);
    pthread_create(&t_dis, NULL, discharge_listener_thread, NULL);
    pthread_create(&t_nurse, NULL, nurse_thread, NULL);		// single dynamic nurse sweeps memory

    while (keep_running) pause();	// main thread sleeps until sigterm
    pthread_join(t_rec, NULL); pthread_join(t_sch, NULL);	// clean up threads safely
    pthread_join(t_dis, NULL); pthread_join(t_nurse, NULL);

    fclose(mem_log);
    msync(mmap_region, MMAP_SIZE, MS_SYNC);	// flush virtual memory changes to disk
    munmap(mmap_region, MMAP_SIZE);	// clean up mmap mapping
    close(mmap_fd);

    shmdt(shm_ptr); shmctl(shmid, IPC_RMID, NULL);	// destroy shared memory
    return 0;
}
