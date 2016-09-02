#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdint.h>

const int64_t NANOS_PER_SEC = 1000000000;

# define TIMEVAL_TO_TIMESPEC(tv, ts) {                                   \
    (ts)->tv_sec = (tv)->tv_sec;                                    \
    (ts)->tv_nsec = (tv)->tv_usec * 1000;                           \
}
# define TIMESPEC_TO_TIMEVAL(tv, ts) {                                   \
    (tv)->tv_sec = (ts)->tv_sec;                                    \
    (tv)->tv_usec = (ts)->tv_nsec / 1000;                           \
}

/* Convert nanoseconds to a timespec */
struct timespec nanos_to_timespec(int64_t nanos) {
  struct timespec t;
  int64_t dnanos   = nanos % NANOS_PER_SEC;
  int64_t dseconds = (nanos - dnanos) / NANOS_PER_SEC;
  t.tv_nsec = dnanos;
  t.tv_sec = dseconds;
  return t;
}

/* Convert a timespec to nanos. Int64_t is probably good enough. This code
 * won't last two hundred years. :) */
int64_t nanos timespec_to_nanos(struct timespec t) {
  int64_t nanos = t.tv_sec;
  nanos *= NANOS_PER_SEC;
  nanos += t.tv_nsec;
  return nanos;
}

/* Obtain monotonic clock as a timespec */
struct timespec monotonic_now() {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return now;
}

/* Obtain wall clock as a timespec */
struct timespec wall_now() {
  struct timespec now_ts;
  struct timeval  now_tv;
  struct timezone tz;
  if (0 != gettimeofday(&now_tv, &tz)) {
    perror("gettimeofday");
    exit(1);
  }
  TIMEVAL_TO_TIMESPEC(&now_tv, &now_ts);
  return now_ts;
}

/* Obtain wall clock timezone */
struct timezone wall_tz() {
  struct timeval tv;
  struct timezone tz;
  if (0 != gettimeofday(&tv, &tz)) {
    perror("gettimeofday");
    exit(1);
  }
  return tz;
}

/* Set wall clock */
void set_wall_clock(struct timespec ts, struct timezone tz) {
  struct timeval tv;
  TIMESPEC_TO_TIMEVAL(&tv, &ts);
  // printf("Setting clock: %d %d\n", tv.tv_sec, tv.tv_usec);
  if (0 != settimeofday(&tv, &tz)) {
    perror("settimeofday");
    exit(2);
  }
}

/* Rebalances sec/nsec to be within bounds. Mutates t.*/
void balance_timespec_m(struct timespec *t) {
  while (t->tv_nsec <= NANOS_PER_SEC) {
    t->tv_sec -= 1;
    t->tv_nsec += NANOS_PER_SEC;
  }
  while (NANOS_PER_SEC <= t->tv_nsec) {
    t->tv_sec += 1;
    t->tv_nsec -= NANOS_PER_SEC;
  }
}

/* Add two timespecs, returning their sum */
struct timespec add_timespec(struct timespec a, struct timespec b) {
  struct timespec result;
  result.tv_sec = a.tv_sec + b.tv_sec;
  result.tv_nsec = a.tv_nsec + b.tv_nsec;
  balance_timespec_m(&result);
  return result;
}

/* Subtract one timespec from another, returning their difference. */
struct timespec sub_timespec(struct timespec a, struct timespec b) {
  struct timespec result;
  result.tv_sec = a.tv_sec - b.tv_sec;
  result.tv_nsec = a.tv_nsec - b.tv_nsec;
  balance_timespec_m(&result);
  return result;
}

/* Modulo timespecs */
struct timespec mod_timespec(struct timespec a, struct timespec n) {
  return nanos_to_timespec(timespec_to_nanos(a) % timespec_to_nanos(n));
}

/* Standard -1, 0, +1 comparator over timespecs */
int8_t cmp_timespec(struct timespec a, struct timespec b) {
  if (a.tv_sec < b.tv_sec) {
    return 1;
  } else if (b.tv_sec < a.tv_sec) {
    return -1;
  } else {
    if (a.tv_nsec < b.tv_nsec) {
      return 1;
    } else if (b.tv_nsec < a.tv_nsec) {
      return -1;
    } else {
      return 0;
    }
  }
}

/* Given a timespec dt, and an anchor timespec (on the monotonic clock), finds
 * the next monotonic timespec tick = anchor + n * dt, where n is some integer,
 * such that tick is greater than now. */
struct timespec next_tick(struct timespec dt, struct timespec anchor, struct timespec now) {
  return add_timespec(
      sub_timespec(
        dt,
        mod_timespec(sub_timespec(now, anchor), dt)));
}

void sleep_until_next_tick(struct timespec dt, struct timespec anchor) {
  struct timespec now = monotonic_now();
  struct timespec next_tick = next_tick(dt, anchor, now);
  struct timespec delta = sub_timespec(next_tick, now);
  if (0 != nanosleep(&delta, null)) {
    perror("nanosleep");
    exit(3);
  }
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s <delta> <period> <duration>\n", argv[0]);
    fprintf(stderr, "Delta and period are in ms, duration is in seconds. "
        "Every period ms, adjusts the clock forward by delta ms, or, "
        "alternatively, back by delta ms. Does this for duration seconds, "
        "then exits. Useful for confusing the heck out of systems that "
        "assume clocks are monotonic and linear.\n");
    return 1;
  }

  /* Parse args */
  struct timespec delta     = nanos_to_timespec(atof(argv[1]) * 1000000);
  struct timespec period    = nanos_to_timespec(atof(argv[2]) * 1000000);
  struct timespec duration  = nanos_to_timespec(atof(argv[3]) * 1000000000);

  /* How far ahead of the monotonic clock is wall time? */
  struct timespec normal_offset = sub_timespec(wall_now(), monotonic_now());
  struct timespec weird_offset  = add_timespec(normal_offset, delta);

  /* We'll need the timezone to set the clock later */
  struct timezone tz = wall_tz();

  /* And somewhere to store nanosleep remainders */
  struct timespec rem;

  /* When (in monotonic time) should we stop changing the clock? */
  struct timespec end = add_timespec(monotonic_now(), duration);

  /* Are we in weird time mode or not? */
  int8_t weird = 0;

  /* Number of adjustments */
  int64_t count = 0;

  /* Strobe the clock until duration's up! */
  while (0 < cmp_timespec(monotonic_now(), end)) {
    set_wall_clock(add_timespec(monotonic_now(),
                                (weird ? normal_offset : weird_offset)),
                   tz);
    // printf("Time now:      %d %d\n", wall_now().tv_sec, wall_now().tv_nsec);
    weird = !weird;
    count += 1;

    if (0 != nanosleep(&period, &rem)) {
      perror("nanosleep");
      exit(3);
    }
  }

  /* Reset clock and print number of changes */
  set_wall_clock(add_timespec(monotonic_now(), normal_offset), tz);
  printf("%d\n", count);
  return 0;
}
