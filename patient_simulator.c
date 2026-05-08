#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

int main(int argc, char* argv[]) {
    if (argc < 5) {
        printf("Usage: ./patient_simulator <id> <priority> <bed_id> <burst_time>\n");
        return 1;
    }

    int patient_id = atoi(argv[1]);
    int priority = atoi(argv[2]);
    int bed_id = atoi(argv[3]);
    int burst_time = atoi(argv[4]);
    printf("[Patient %d] Arrived. Triage Priority: %d. Assigned to bed %d.\n",	// Arrival and Treatment Notification
           patient_id, priority, bed_id);
    printf("[Patient %d] Treatment started. Duration: %d seconds.\n",
           patient_id, burst_time);

    sleep(burst_time);	// Simulate Treatment Duration
    printf("[Patient %d] Treatment complete. Discharging.\n", patient_id);	// Discharge Notification

    int fd = open("/tmp/discharge_fifo", O_WRONLY);	// Notify Admissions Manager via Named Pipe
    if (fd != -1){
        char msg[64];
        snprintf(msg, sizeof(msg), "%d,%d\n", patient_id, bed_id);	// Format: <patient_id>,<bed_id>
        write(fd, msg, strlen(msg));
        close(fd);
    } else {
        perror("Failed to open discharge_fifo");
    }

    return 0;
}
