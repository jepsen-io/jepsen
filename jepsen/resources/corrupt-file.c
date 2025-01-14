#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>

/* Generates the starting offset of a chunk that we *won't* overwrite. */
off_t rand_source_offset(off_t chunk_size, uint32_t node_mod, uint32_t node_index,
    off_t file_size) {
  off_t chunk_count = file_size / chunk_size;
  off_t chunk = node_index;
  // Generate a random chunk that we *won't* use
  while ((chunk % node_mod) == node_index) {
    /* fprintf(stderr, "rerolling %ld\n", chunk); */
    chunk = rand() % chunk_count;
  }
  /* fprintf(stderr, "final chunk: %ld\n", chunk); */
  return chunk * chunk_size;
}

/* Ugh I am SO bad at C numbers, please forgive me. I intend to do everything
 * here supporting large (e.g. terabyte) file sizes; hopefully that's how it
 * works out */
int corrupt(off_t chunk_size, uint32_t node_mod, uint32_t node_index,
    char* file) {
  /* Open file */
  int fd = open(file, O_RDWR);
  if (fd == -1) {
    printf("fopen() failed\n");
    return 2;
  }

  /* How big is this file? */
  struct stat finfo;
  if (fstat(fd, &finfo) != 0) {
    fprintf(stderr, "fstat failed: %s\n",
        strerror(errno));
    close(fd);
    return 3;
  }
  off_t file_size = finfo.st_size;
  /* fprintf(stderr, "file is %lu bytes\n", file_size); */

  /* We start at chunk node_index and jump up by node_mod each time */
  off_t offset = chunk_size * node_index;
  /* because copy_file_range mutates the offset we pass it */
  off_t throwaway_offset;
  off_t source_offset;
  ssize_t copied;
  while (offset < file_size) {
    source_offset = rand_source_offset(chunk_size, node_mod, node_index,
        file_size);
    /* fprintf(stderr, "copy %lu\t-> %lu ...\t", source_offset, offset); */
    throwaway_offset = offset;
    copied = copy_file_range(fd, &source_offset, fd, &throwaway_offset,
        chunk_size, 0);
    /* fprintf(stderr, "%ld bytes copied\n", copied); */
    if (copied == -1) {
      fprintf(stderr, "copy error: %s\n", strerror(errno));
      close(fd);
      return 4;
    }
    offset = offset + (chunk_size * node_mod);
  }

  close(fd);
  return 0;
}

int main (int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "usage: %s <chunk_size> <modulus> <index> <file>\n",
        argv[0]);
    fprintf(stderr,
        "Takes a chunk size in bytes, a modulus m, an index i, and a file. "
        "Divides file into chunks, each chunk_size bytes, and overwrites "
        "all chunks where `(chunk-index mod m) = i` with garbage.\n");
    return 1;
  }

  /* Parse args */
  off_t chunk_size = strtol(argv[1], NULL, 10);
  uint32_t node_mod = strtol(argv[2], NULL, 10);
  uint32_t node_index = strtol(argv[3], NULL, 10);
  char* file = argv[4];

  if (node_mod <= 0) {
    fprintf(stderr, "node_mod must be greater than zero");
    return 1;
  }

  if (node_mod <= node_index) {
    fprintf(stderr, "node_index must be less than node_mod");
    return 1;
  }

  srand(time(NULL));

  int ret = corrupt(chunk_size, node_mod, node_index, file);
  return ret;
}
