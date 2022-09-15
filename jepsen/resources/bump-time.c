#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>

int main(int argc, char **argv) {
    if (argc < 2)
    {
        fprintf(stderr, "usage: %s <delta>, where delta is in ms\n", argv[0]);
        return 1;
    }

    /* Compute offset from argument */
    int64_t delta    = atof(argv[1]) * 1000000;
    int64_t delta_ns = delta % 1000000000;
    int64_t delta_s  = (delta - delta_ns) / 1000000000;

    /* Get current time */
    /*struct timeval time;*/
    /*struct timezone tz;*/

    /*if (0 != gettimeofday(&time, &tz)) {*/
    /*  perror("gettimeofday");*/
    /*  return 1;*/
    /*}*/

    struct timespec time;
    if (0 != clock_gettime(CLOCK_REALTIME, &time)) {
      perror("clock_gettime");
      return 1;
    }

    /* Update time */
    time.tv_sec += delta_s;
    time.tv_nsec += delta_ns;
    /* Overflow */
    while (time.tv_nsec <= 1000000000) {
      time.tv_sec -= 1;
      time.tv_nsec += 1000000000;
    }
    while (1000000000 <= time.tv_nsec) {
      time.tv_sec += 1;
      time.tv_nsec -= 1000000000;
    }

    /* Set time */
    if (0 != clock_settime(CLOCK_REALTIME, &time)) {
      perror("clock_settime");
      return 2;
    }

    /* Print current time */
    if (0 != clock_gettime(CLOCK_REALTIME, &time)) {
      perror("clock_gettime");
      return 1;
    }
    fprintf(stdout, "%d.%09d\n", time.tv_sec, time.tv_nsec);
    return 0;
}
