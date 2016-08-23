#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc < 2)
    {
	fprintf(stderr, "usage: %s <delta>\n", argv[0]);
	return 1;
    }
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = atof(argv[1]) * 1000;
    if (0 != adjtime(&tv, 0)) {
	perror("adjtime");
	return 1;
    }
    return 0;
}
