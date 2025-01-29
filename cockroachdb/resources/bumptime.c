#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdint.h>

int main(int argc, char **argv) {
    if (argc < 2)
    {
        fprintf(stderr, "usage: %s <delta>, where delta is in ms\n", argv[0]);
        return 1;
    }

    /* Compute offset from argument */
    int64_t delta    = atof(argv[1]) * 1000;
    int64_t delta_us = delta % 1000000;
    int64_t delta_s  = (delta - delta_us) / 1000000;

    /* Get current time */
    struct timeval time;
    struct timezone tz;

    if (0 != gettimeofday(&time, &tz)) {
      perror("gettimeofday");
      return 1;
    }

    /* Update time */
    time.tv_usec += delta_us;
    time.tv_sec += delta_s;
    /* Overflow */
    while (time.tv_usec <= 1000000) {
      time.tv_sec -= 1;
      time.tv_usec += 1000000;
    }
    while (1000000 <= time.tv_usec) {
      time.tv_sec += 1;
      time.tv_usec -= 1000000;
    }

    /* Set time */
    if (0 != settimeofday(&time, &tz)) {
      perror("settimeofday");
      return 2;
    }

    return 0;
}
