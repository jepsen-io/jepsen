#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#define OFF_MAX (sizeof(off_t) == sizeof(long long) ? LLONG_MAX : sizeof(off_t) == sizeof(int) ? INT_MAX : -999999)
#define OFF_MIN (sizeof(off_t) == sizeof(long long) ? LLONG_MIN : sizeof(off_t) == sizeof(int) ? INT_MIN : -999999)


#include <argp.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static char doc[] =
  "Corrupts a file on disk, for testing the safety of distributed databases.\n"
  "\n"
  "Takes a chunk size in bytes, a modulus m, an index i, and a file. "
  "Divides file into chunks, each chunk_size bytes, and overwrites "
  "all chunks where `(chunk-index mod m) == i` with garbage, selected "
  "from other chunks of the file. The range within the file can be limited "
  "with --start and --end.";

const char *argp_program_version = "corrupt-file 0.0.1";
const char *argp_program_bug_address = "<aphyr@jepsen.io>";

/* We take one argument: a file to corrupt. */
static char args_doc[] = "FILE";

/* Our options */
static struct argp_option opt_spec[] = {
  {"start", 1, "BYTES", 0, "Index into the file, in bytes, inclusive, where corruption starts. Default 0."},
  {"end", 2, "BYTES", 0, "Index into the file, in bytes, exclusive, where corruption stops. Defaults to the largest file offset on this platform."},
  {"chunk-size", 'c', "BYTES", 0, "The size of each chunk, in bytes. Default 1 MB."},
  {"index", 'i', "INDEX", 0, "The index of the first chunk to corrupt. 0 means the first chunk, starting from --start. Default 0."},
  {"modulus", 'm', "MOD", 0, "After index, corrupt every MOD chunks. 3 means every third chunk. Default 1."},
  { 0 }
};

/* We bundle these options into a single structure, for passing around */
struct opts {
  char *file;
  off_t start;
  off_t end;
  off_t chunk_size;
  uint32_t index;
  uint32_t mod;
};

/* Constructs a default options map */
struct opts default_opts() {
  struct opts opts;
  opts.start = 0;
  opts.end = OFF_MAX;
  opts.chunk_size = 1024 * 1024; // 1 MB
  opts.index = 0;                // Starting at 0
  opts.mod = 1;                  // Every chunk
  return opts;
}

/* Print an options map */
void print_opts(struct opts opts) {
  fprintf(stderr, "{:start      %ld,\n"
      " :end        %ld,\n"
      " :chunk_size %ld,\n"
      " :index      %d,\n"
      " :mod        %d,\n"
      " :file       \"%s\"}\n",
      opts.start, opts.end, opts.chunk_size, opts.index, opts.mod, opts.file);
}

/* Validate an options map. Returns 0 for OK, any other number for an error. */
int validate_opts(struct opts opts) {
  if (opts.start < 0) {
    fprintf(stderr, "start %ld must be 0 or greater\n", opts.start);
    return 1;
  }

  if (opts.end < 0) {
    fprintf(stderr, "end %ld must be 0 or greater\n", opts.end);
    return 1;
  }

  if (OFF_MAX < opts.end) {
    fprintf(stderr, "end %ld must be less than OFF_MAX (%lld)\n",
        opts.end, OFF_MAX);
    return 1;
  }

  if (opts.end < opts.start) {
    fprintf(stderr, "start %ld must be less than or equal to end %ld\n",
        opts.start, opts.end);
    return 1;
  }

  if ((opts.index < 0) || (opts.mod <= opts.index)) {
    fprintf(stderr, "index %u must fall in [0, %u)\n",
        opts.index, opts.mod);
    return 1;
  }

  if (opts.chunk_size <= 0) {
    fprintf(stderr, "chunk size %ld must be positive\n", opts.chunk_size);
    return 1;
  }

  return 0;
}

/* Called at each step of option parsing */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  //fprintf(stderr, "parse_opt key %i, arg %s\n", key, arg);

  /* Fetch input from argp_parse: a pointer to our opts structure */
  struct opts *opts = state->input;

  // What option (or phase of parsing) are we at?
  switch (key) {
    case 1:
      // So is a long long big enough for an off_t or not? Someone who actually
      // understands C integer sizes, please fix this
      opts->start = strtoll(arg, NULL, 10);
      break;
    case 2:
      opts->end = strtoll(arg, NULL, 10);
      break;
    case 'c':
      opts->chunk_size = strtoll(arg, NULL, 10);
      break;
    case 'i':
      // TODO: This will overflow when given negative ints
      opts->index = strtol(arg, NULL, 10);
      break;
    case 'm':
      // TODO: This will overflow when given negative ints
      opts->mod = strtol(arg, NULL, 10);
      break;
    case ARGP_KEY_ARG:
      if (1 < state->arg_num) {
        /* Too many args */
        argp_usage(state);
      }
      opts->file = arg;
      break;
    case ARGP_KEY_END:
      if (state->arg_num < 1) {
        // Not enough args
        argp_usage(state);
      }
      break;
    default:
      return ARGP_ERR_UNKNOWN;
    }
  return 0;
}

/* Argument parser */
static struct argp argp = { opt_spec, parse_opt, args_doc, doc };

/* Generates a random off_t in [0, max) */
off_t rand_int(off_t max) {
  // Not well-dispersed, but whatever
  return (rand() % max);
}

/* Returns the offset of a given chunk. */
off_t chunk_offset(struct opts opts, off_t chunk) {
  return opts.start + (chunk * opts.chunk_size);
}

/* How many chunks can fit (without running over) the region in this file? */
off_t chunk_count(struct opts opts, off_t file_size) {
  off_t end = opts.end;
  if (file_size < end) {
    end = file_size;
  }
  if (end < opts.start) {
    return 0;
  }
  return (end - opts.start) / opts.chunk_size;
}

/* Generates the starting offset of a chunk. Try to prefer chunks that we
 * won't corrupt. This is impossible if there is only one chunk or we intend
 * to corrupt every chunk. In the latter case, we choose random chunks. */
off_t rand_source_offset(struct opts opts, off_t dest_offset, off_t file_size) {
  off_t chunk_count_ = chunk_count(opts, file_size);
  /* Not sure exactly what to do here. There's only 0 or 1 chunks. We *want* to
   * corrupt something. But we can't corrupt it by copying another chunk! */
  if (chunk_count_ < 2) {
    return -1;
  }

  /* Start with a random chunk */
  off_t chunk = rand_int(chunk_count_);

  if (opts.mod == 1) {
    /* We're corrupting every chunk; there are no clean chunks to choose from.
       Any location will do. */
    while (chunk_offset(opts, chunk) == dest_offset) {
      chunk = rand_int(chunk_count_);
    }
  } else {
    /* We're not corrupting every chunk. Choose an unaffected one. */
    while ((chunk % opts.mod) == opts.index) {
      // fprintf(stderr, "rerolling %ld\n", chunk);
      chunk = rand_int(chunk_count_);
    }
  }

  // fprintf(stderr, "final chunk: %ld\n", chunk);
  return chunk_offset(opts, chunk);
}

/* Ugh I am SO bad at C numbers, please forgive me. I intend to do everything
 * here supporting large (e.g. terabyte) file sizes; hopefully that's how it
 * works out */
int corrupt(struct opts opts) {
  /* Open file */
  int fd = open(opts.file, O_RDWR);
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
  off_t chunk_count_ = chunk_count(opts, file_size);
  // fprintf(stderr, "file is %lu bytes, %lu chunks\n", file_size, chunk_count_);

  off_t source_offset;
  off_t dest_offset;
  ssize_t copied;

  // Stats
  off_t bytes_corrupted = 0;
  off_t chunks_corrupted = 0;

  for (off_t chunk = opts.index; chunk < chunk_count_; chunk += opts.mod) {
    dest_offset = chunk_offset(opts, chunk);

    // Negative offset indicates there are no other chunks we can copy from
    source_offset = rand_source_offset(opts, dest_offset, file_size);
    if (0 <= source_offset) {
      // fprintf(stderr, "copy %lu\t-> %lu ...\t", source_offset, dest_offset);
      copied = copy_file_range(fd, &source_offset, fd, &dest_offset,
          opts.chunk_size, 0);
      // fprintf(stderr, "%ld bytes copied\n", copied);
      if (copied == -1) {
        fprintf(stderr, "copy error: %s\n", strerror(errno));
        close(fd);
        return 4;
      }

      bytes_corrupted += copied;
      chunks_corrupted += 1;
    }
  }

  close(fd);

  fprintf(stdout, "Corrupted %ld chunks (%ld bytes)\n",
      chunks_corrupted, bytes_corrupted);
  return 0;
}

/* Go go go! */
int main (int argc, char **argv) {
  /* Parse args */
  struct opts opts = default_opts();
  error_t err = argp_parse (&argp, argc, argv, 0, 0, &opts);
  if (err != 0) {
    fprintf(stderr, "Error parsing args: %d\n", err);
    return 1;
  }
  int err2 = validate_opts(opts);
  if (err2 != 0) {
    return err2;
  }
  // print_opts(opts);

  // Init rand
  srand(time(NULL));

  // Go
  return corrupt(opts);
}
