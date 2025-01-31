#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#define OFF_MAX (sizeof(off_t) == sizeof(long long) ? LLONG_MAX : sizeof(off_t) == sizeof(int) ? INT_MAX : -999999)
#define OFF_MIN (sizeof(off_t) == sizeof(long long) ? LLONG_MIN : sizeof(off_t) == sizeof(int) ? INT_MIN : -999999)

#include <argp.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static char doc[] =
  "Corrupts a file on disk, for testing database safety.\n"
  "\n"
  "Takes a `file`. Affects a region of bytes within that file: "
  "[`start`, `end`). Divides this region into chunks, each `chunk-size` "
  "bytes. Numbering those chunks 0, 1, ..., affects every `modulus` "
  "chunks, starting with chunk number `index`. The `mode` flag determines "
  "what we do to those chunks: copying them around, flipping bits, taking "
  "and restoring snapshots, etc.";

const char *argp_program_version = "corrupt-file 0.0.1";
const char *argp_program_bug_address = "<aphyr@jepsen.io>";

/* We take one argument: a file to corrupt. */
static char args_doc[] = "FILE";

/* Our exit statuses. Should I just use ERRNO codes? I don't really know the
 * conventions. */
const int EXIT_OK = 0;    // Fine
const int EXIT_ARGS = 1;  // Argument parsing problem
const int EXIT_IO = 2;    // IO error
const int EXIT_INT = 3;   // Some sort of internal error, like string concat

/* Our options */
#define OPT_START 1
#define OPT_END 2
#define OPT_MODULUS 3
#define OPT_CLEAR_SNAPSHOTS 4

static struct argp_option opt_spec[] = {
  {"chunk-size", 'c', "BYTES", 0,
    "The size of each chunk, in bytes. Default 1 MB."},
  {"clear-snapshots", OPT_CLEAR_SNAPSHOTS, NULL, 0,
    "If set, wipes out the entire snapshot directory before doing "
    "anything else. This can be run without any file."},
  {"end", OPT_END, "BYTES", 0,
    "Index into the file, in bytes, exclusive, where corruption stops. "
    "Defaults to the largest file offset on this platform."},
  {"index", 'i', "INDEX", 0,
    "The index of the first chunk to corrupt. 0 means the first chunk, "
    "starting from --start. Default 0."},
  {"mode", 'm', "MODE", 0,
    "What to do with affected regions of the file. "
    "Use `copy` to replace a chunk with some other chunk. Use `bitflip` to "
    "flip random bits with a per-bit `--probability`. Use `snapshot` to "
    "take a snapshot of the chunk for use later, leaving the chunk unchanged. "
    "Snapshots are stored in `/tmp/jepsen/corrupt-file/snapshots/`. Use "
    "`restore` to restore snapshots (when available). If -m is not provided, "
    "does not corrupt the file."},
  {"modulus", OPT_MODULUS, "MOD", 0,
    "After index, corrupt every MOD chunks. 3 means every third chunk. "
    "Default 1: every chunk"},
  {"probability", 'p', "PROB", 0,
    "For --mode bitflip, determines the probability that any given bit "
    "in the file flips. Default 1e-6: roughly eight errors per megabyte."},
  {"start", OPT_START, "BYTES", 0,
    "Index into the file, in bytes, inclusive, where corruption starts. "
      "Default 0."},
  { 0 }
};

/* Different modes we can run in. Love too see. */
#define MODE_NONE 0
#define MODE_COPY 1
#define MODE_SNAPSHOT 2
#define MODE_RESTORE 3
#define MODE_BITFLIP 4

/* Where do we stash snapshots? */
static char SNAPSHOT_DIR[] = "/tmp/jepsen/corrupt-file/snapshots";

/* We bundle these options into a single structure, for passing around */
struct opts {
  char *file;
  int mode;
  off_t start;
  off_t end;
  off_t chunk_size;
  uint32_t index;
  uint32_t mod;
  double probability;
  bool clear_snapshots;
};

/* Constructs a default options map */
struct opts default_opts() {
  struct opts opts;
  opts.mode = MODE_NONE;
  opts.start = 0;
  opts.end = OFF_MAX;
  opts.chunk_size = 1024 * 1024; // 1 MB
  opts.index = 0;                // Starting at 0
  opts.mod = 1;                  // Every chunk
  opts.probability = 1e-6;       // Roughly 1/MB
  opts.clear_snapshots = false;
  return opts;
}

/* Print an options map */
void print_opts(struct opts opts) {
  fprintf(stderr,
      "{:mode             %d,\n"
      " :start            %ld,\n"
      " :end              %ld,\n"
      " :chunk_size       %ld,\n"
      " :index            %d,\n"
      " :mod              %d,\n"
      " :probability      %f.\n"
      " :file             \"%s\"\n"
      " :clear_snaphots   %d}\n",
      opts.mode, opts.start, opts.end, opts.chunk_size, opts.index, opts.mod,
      opts.probability, opts.file, opts.clear_snapshots);
}

/* Validate an options map. Returns EXIT_OK for OK, EXIT_ARGS for an error. */
int validate_opts(struct opts opts) {
  if (opts.start < 0) {
    fprintf(stderr, "start %ld must be 0 or greater\n", opts.start);
    return EXIT_ARGS;
  }

  if (opts.end < 0) {
    fprintf(stderr, "end %ld must be 0 or greater\n", opts.end);
    return EXIT_ARGS;
  }

  if (OFF_MAX < opts.end) {
    fprintf(stderr, "end %ld must be less than OFF_MAX (%lld)\n",
        opts.end, OFF_MAX);
    return EXIT_ARGS;
  }

  if (opts.end < opts.start) {
    fprintf(stderr, "start %ld must be less than or equal to end %ld\n",
        opts.start, opts.end);
    return EXIT_ARGS;
  }

  if ((opts.index < 0) || (opts.mod <= opts.index)) {
    fprintf(stderr, "index %u must fall in [0, %u)\n",
        opts.index, opts.mod);
    return EXIT_ARGS;
  }

  if (opts.probability < 0.0 || 1.0 < opts.probability) {
    fprintf(stderr, "Probability %f must be within [0,1]", opts.probability);
    return EXIT_ARGS;
  }

  if (opts.chunk_size <= 0) {
    fprintf(stderr, "chunk size %ld must be positive\n", opts.chunk_size);
    return EXIT_ARGS;
  }

  return EXIT_OK;
}

/* Called at each step of option parsing */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  //fprintf(stderr, "parse_opt key %i, arg %s\n", key, arg);

  /* Fetch input from argp_parse: a pointer to our opts structure */
  struct opts *opts = state->input;

  // What option (or phase of parsing) are we at?
  switch (key) {
    case OPT_START:
      // So is a long long big enough for an off_t or not? Someone who actually
      // understands C integer sizes, please fix this
      opts->start = strtoll(arg, NULL, 10);
      break;
    case OPT_END:
      opts->end = strtoll(arg, NULL, 10);
      break;
    case OPT_MODULUS:
      // TODO: This will overflow when given negative ints
      opts->mod = strtol(arg, NULL, 10);
      break;
    case OPT_CLEAR_SNAPSHOTS:
      opts->clear_snapshots = true;
      break;
    case 'c':
      opts->chunk_size = strtoll(arg, NULL, 10);
      break;
    case 'i':
      // TODO: This will overflow when given negative ints
      opts->index = strtol(arg, NULL, 10);
      break;
    case 'm':
      if (strcmp(arg, "copy") == 0) {
        opts->mode = MODE_COPY;
      } else if (strcmp(arg, "bitflip") == 0) {
        opts->mode = MODE_BITFLIP;
      } else if (strcmp(arg, "snapshot") == 0) {
        opts->mode = MODE_SNAPSHOT;
      } else if (strcmp(arg, "restore") == 0) {
        opts->mode = MODE_RESTORE;
      } else {
        argp_error(state, "Unknown mode %s", arg);
      }
      break;
    case 'p':
      opts->probability = strtod(arg, NULL);
      break;
    case ARGP_KEY_ARG:
      if (1 < state->arg_num) {
        /* Too many args */
        argp_usage(state);
      }
      opts->file = realpath(arg, NULL);
      break;
    case ARGP_KEY_END:
      if (state->arg_num < 1 && !(opts->clear_snapshots)) {
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

/* Utilities */

/* Generates a uniform random off_t in [0, max) */
off_t rand_int(off_t max) {
  if (0 < max) {
    // Not well-dispersed, but whatever.
    // TODO: this is only from [0, 2^31). Need to roll twice to get a full
    // off_t.
    return (lrand48() % max);
  } else {
    return 0;
  }
}

/* Generates a random exponentially distributed off_t\, with rate parameter
 * lambda. */
off_t rand_exp_int(double lambda) {
  double u = drand48();
  return (-1 / lambda) * log(u);
}

/* Create directory, recursively. Returns an error, or 0. */
int mkdir_p(const char *path) {
  char *path_  = strdup(path);
  char *parent = dirname(path_);
  int res = 0;
  if (strlen(parent) > 1) {
    res = mkdir_p(parent);
  }
  // Why does freeing these corrupt memory? I'm so confused.
  //free(parent);
  //free(path_);
  if (res != 0 && errno != EEXIST) {
    return errno;
  }
  res = mkdir(path, S_IWUSR | S_IRUSR | S_IXUSR);
  if (res != 0 && errno != EEXIST) {
    return errno;
  }
  return 0;
}

/* Working with chunks */

/* Returns the offset of a given chunk. */
off_t chunk_offset(struct opts opts, off_t chunk) {
  return opts.start + (chunk * opts.chunk_size);
}

/* How many chunks can fit (without running over) the region in this file? */
off_t chunk_count(struct opts opts, off_t file_size) {
  off_t start = opts.start;
  off_t end = opts.end;
  if (file_size < end) {
    end = file_size;
  }
  if (end < start) {
    return 0;
  }
  off_t region_size = end - start;
  // First, with rounding
  off_t chunks = region_size / opts.chunk_size;
  // One extra chunk if there's a remainder
  if (0 != (region_size % opts.chunk_size)) {
    chunks += 1;
  }
  return chunks;
}

/* Takes a filename and [start, end) offsets within it. Computes the path to a
 * file where we can store a snapshot of it. */
char *snapshot_path(char* file, off_t start, off_t end) {
  char *buf = malloc(PATH_MAX + 1);
  int written = snprintf(buf, PATH_MAX, "%s/%s:%ld:%ld", SNAPSHOT_DIR, file, start, end);
  if (written < 0) {
    fprintf(stderr, "error writing string: %d", written);
  }
  return buf;
}

/* Corrupt (well, really, just save chunks of) a file by copying chunks to
 * files in /tmp, to be restored later. */
int corrupt_snapshot(struct opts opts, int fd, off_t file_size, off_t
    chunk_count) {

  // Make snapshot directory
  char *snapshot = snapshot_path(opts.file, 0, 0);
  char *dir = dirname(snapshot);
  // fprintf(stderr, "Making directory %s\n", dir);
  int err = mkdir_p(dir);
  if (err != 0) {
    fprintf(stderr, "Creating directory %s failed: %s\n", dir, strerror(err));
    return EXIT_IO;
  }
  //free(snapshot);

  // Destination file
  int dest_fd;

  // Chunk addresses
  off_t start = 0;
  off_t end = 0;

  // Stats
  ssize_t copied = 0;
  off_t bytes_snapped = 0;
  off_t chunks_snapped = 0;

  for (off_t chunk = opts.index; chunk < chunk_count; chunk += opts.mod) {
    // Where are we corrupting?
    start = chunk_offset(opts, chunk);
    end   = end + opts.chunk_size;
    // Don't read off the end of the region
    if (opts.end < end) {
      end = opts.end;
    }

    // Open snapshot file
    snapshot = snapshot_path(opts.file, start, end);
    // fprintf(stderr, "Snapshot is %s\n", snapshot);
    err = unlink(snapshot);
    if (err != 0 && errno != ENOENT) {
      fprintf(stderr, "unlink() failed: %s (%d)", strerror(errno), errno);
      // Fuck me, how do people manage memory leaks with early return? This
      // feels awful
      //free(dir);
      return EXIT_IO;
    }
    dest_fd = open(snapshot, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
    if (dest_fd == -1) {
      fprintf(stderr, "open() failed\n");
      //free(snapshot);
      //free(dir);
      return EXIT_IO;
    }

    // Snapshot region
    copied = copy_file_range(fd, &start, dest_fd, NULL, end - start, 0);
    if (copied == -1) {
      fprintf(stderr, "copy error: %s\n", strerror(errno));
      close(fd);
      close(dest_fd);
      //free(snapshot);
      //free(dir);
      return EXIT_IO;
    }

    // Stats
    bytes_snapped += copied;
    chunks_snapped += 1;

    // Clean up
    close(dest_fd);
    //free(snapshot);
  }
  fprintf(stdout, "Snapshot %ld chunks (%ld bytes)\n",
      chunks_snapped, bytes_snapped);

  //free(dir);
  return EXIT_OK;
}

/* Corrupt chunks of a file by restoring them from snapshot files in /tmp. */
int corrupt_restore(struct opts opts, int fd, off_t file_size, off_t
    chunk_count) {

  // Source file
  char *snapshot;
  int source_fd;

  // Chunk addresses
  off_t start = 0;
  off_t end = 0;

  // Stats
  ssize_t copied = 0;
  off_t bytes_restored = 0;
  off_t chunks_restored = 0;

  for (off_t chunk = opts.index; chunk < chunk_count; chunk += opts.mod) {
    // Where are we corrupting?
    start = chunk_offset(opts, chunk);
    end   = end + opts.chunk_size;
    // Don't write past the end of the region
    if (opts.end < end) {
      end = opts.end;
    }

    // Open snapshot file
    snapshot = snapshot_path(opts.file, start, end);
    // fprintf(stderr, "Snapshot is %s\n", snapshot);
    source_fd = open(snapshot, O_RDONLY);
    if (source_fd == -1) {
      if (errno == ENOENT) {
        // That's fine, we didn't snapshot this block.
        free(snapshot);
        continue;
      }
      fprintf(stderr, "open() failed\n");
      free(snapshot);
      return EXIT_IO;
    }

    // Restore chunk
    copied = copy_file_range(source_fd, 0, fd, &start, end - start, 0);
    if (copied == -1) {
      fprintf(stderr, "copy error: %s\n", strerror(errno));
      close(source_fd);
      close(fd);
      free(snapshot);
      return EXIT_IO;
    }

    // Stats
    bytes_restored += copied;
    chunks_restored += 1;

    // Clean up
    close(source_fd);
    free(snapshot);
  }
  fprintf(stdout, "Restored %ld chunks (%ld bytes)\n",
      chunks_restored, bytes_restored);

  return EXIT_OK;
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

/* Corrupt by copying chunks from other chunks */
int corrupt_copy(struct opts opts, int fd, off_t file_size, off_t chunk_count) {
  off_t source_offset;
  off_t start;      // Start of current chunk
  off_t end;        // End of current chunk
  off_t chunk_size; // Size of current chunk
  ssize_t copied;

  // Stats
  off_t bytes_corrupted = 0;
  off_t chunks_corrupted = 0;

  for (off_t chunk = opts.index; chunk < chunk_count; chunk += opts.mod) {
    start = chunk_offset(opts, chunk);
    end = start + opts.chunk_size;
    // Don't go past the end of the file or region
    if (file_size < end) {
      end = file_size;
    }
    if (opts.end < end) {
      end = opts.end;
    }
    chunk_size = end - start;

    // Negative offset indicates there are no other chunks we can copy from
    source_offset = rand_source_offset(opts, start, file_size);
    if (0 <= source_offset) {
      copied = copy_file_range(fd, &source_offset, fd, &start,
          chunk_size, 0);
      if (copied == -1) {
        close(fd);
        return EXIT_IO;
      }

      bytes_corrupted += copied;
      chunks_corrupted += 1;
    }
  }
  fprintf(stdout, "Corrupted %ld chunks (%ld bytes)\n",
      chunks_corrupted, bytes_corrupted);
  return EXIT_OK;
}

/* Flip random bits in affected chunks. */
int corrupt_bitflip(struct opts opts, int fd, off_t file_size,
    off_t chunk_count) {
  /* We model bitflips as poisson processes with lambda = opts.probability; the
   * distance in bits between flips is therefore an exponentially distributed
   * process with rate parameter lambda. I'm not entirely confident about this
   * approach--it seems to yield a higher number of bitflips than I'd otherwise
   * expect with p=1.0 or 0.5. Small ps look better--this might be a weird
   * behavior around 0 inter-bit distances. */

  // For read/write retvals
  ssize_t ret_size;

  // Start and end of chunk
  off_t start = 0;
  off_t end = 0;
  // Size of this chunk
  off_t chunk_size = 0;
  // Bit to flip
  uint8_t target_bit = 0;

  // In-memory buffer--we go one byte at a time.
  uint8_t buf = 0;
  uint8_t mask = 0; // The mask we xor with

  // Stats
  off_t chunks_processed = 0;
  off_t bits_flipped = 0;

  // Starting chunk
  off_t chunk = opts.index;
  // The next offset, in bit number, relative to the start of the current
  // chunk, which we will flip. This offset works as if chunks were contiguous,
  // rather than spread out by mod-1 chunks. Note that our probability is per
  // *bit*, so we translate this to bytes later. Also note that this will break
  // us if we get within 1/8th of the max file size, which I expect will never
  // happen.
  off_t bit_offset = rand_exp_int(opts.probability);
  // byte_offset is always 1/8th bit_offset
  off_t byte_offset = bit_offset / 8;
  // For later, we're going to advance by this factor.
  off_t bit_offset_delta = 0;

  // Work our way through chunks until we have to flip.
  while (chunk < chunk_count) {
    // How big is this chunk?
    start = chunk_offset(opts, chunk);
    end = start + opts.chunk_size;
    // Don't go past the end of the file or region
    if (file_size < end) {
      end = file_size;
    }
    if (opts.end < end) {
      end = opts.end;
    }
    chunk_size = end - start;

    while (byte_offset < chunk_size) {
      // We're flipping in this chunk.
      target_bit = bit_offset % 8;
      mask = (0x01 << target_bit);

      // Read
      ret_size = pread(fd, &buf, 1, start + byte_offset);
      if (ret_size < 0) {
        fprintf(stderr, "pread() failed: %s\n", strerror(errno));
        return EXIT_IO;
      }
      // Flip
      buf = buf ^ mask;
      // Write
      ret_size = pwrite(fd, &buf, 1, start + byte_offset);
      if (ret_size < 0) {
        fprintf(stderr, "pwrite() failed: %s\n", strerror(errno));
        return EXIT_IO;
      }

      bits_flipped += 1;

      // Roll a new inter-arrival interval
      //
      // This math is wrong, and I don't really know why. With high
      // probabilities, advancing by rand_exp_int(p) tends to over-flip, and
      // advancing by 1+rand_exp_int(p) tends to under-flip. There's something
      // here, I think, about what happens when inter-arrival times are zero.
      // My Weird Hack, which is totally unjustifiable, is to force 0 -> 1.
      // This brings us closer to the expected number of flips per file (and
      // stops us from flipping the same bit redundantly) but I think it's
      // still wrong.
      bit_offset_delta = rand_exp_int(opts.probability);
      if (bit_offset_delta == 0) {
        bit_offset_delta = 1;
      }
      bit_offset += bit_offset_delta;
      byte_offset = bit_offset / 8;
    }

    // Next chunk
    chunks_processed += 1;
    bit_offset  -= (chunk_size * 8);
    byte_offset -= chunk_size;
    chunk += opts.mod;
  }

  fprintf(stdout, "Processed %ld chunks (%ld bitflips)\n",
      chunks_processed, bits_flipped);
  return EXIT_OK;
}


/* Ugh I am SO bad at C numbers, please forgive me. I intend to do everything
 * here supporting large (e.g. terabyte) file sizes; hopefully that's how it
 * works out */
int corrupt(struct opts opts) {
  /* Open file */
  int fd = open(opts.file, O_RDWR);
  if (fd == -1) {
    fprintf(stderr, "open() failed\n");
    return EXIT_IO;
  }

  /* How big is this file? */
  struct stat finfo;
  if (fstat(fd, &finfo) != 0) {
    fprintf(stderr, "fstat failed: %s\n",
        strerror(errno));
    close(fd);
    return EXIT_IO;
  }
  off_t file_size = finfo.st_size;
  off_t chunk_count_ = chunk_count(opts, file_size);
  // fprintf(stderr, "file is %lu bytes, %lu chunks\n", file_size, chunk_count_);

  int ret;
  switch (opts.mode) {
    case MODE_COPY:
      ret = corrupt_copy(opts, fd, file_size, chunk_count_);
      break;
    case MODE_SNAPSHOT:
      ret = corrupt_snapshot(opts, fd, file_size, chunk_count_);
      break;
    case MODE_RESTORE:
      ret = corrupt_restore(opts, fd, file_size, chunk_count_);
      break;
    case MODE_BITFLIP:
      ret = corrupt_bitflip(opts, fd, file_size, chunk_count_);
      break;
  }

  close(fd);
  return ret;
}

/* Deletes the snapshot directory recursively. */
int clear_snapshots() {
  size_t limit = sizeof(SNAPSHOT_DIR) + 10;
  char *cmd = malloc(limit);
  int written = snprintf(cmd, limit, "rm -rf '%s'", SNAPSHOT_DIR);
  if (written < 0) {
    fprintf(stderr, "error writing string: %d", written);
    return EXIT_INT;
  }
  int ret = system(cmd);
  free(cmd);
  return ret;
}

/* Go go go! */
int main (int argc, char **argv) {
  /* Parse args */
  struct opts opts = default_opts();
  error_t err = argp_parse (&argp, argc, argv, 0, 0, &opts);
  if (err != 0) {
    fprintf(stderr, "Error parsing args: %d\n", err);
    return EXIT_ARGS;
  }
  int err2 = validate_opts(opts);
  if (err2 != EXIT_OK) {
    return err2;
  }
  //print_opts(opts);

  // Init rand
  srand48(time(NULL));
  srand(time(NULL));

  // Go
  if (opts.clear_snapshots) {
    int exit = clear_snapshots();
    if (exit != EXIT_OK) {
      fprintf(stderr, "Error clearing snapshot directory %s: %d", SNAPSHOT_DIR, exit);
      return EXIT_IO;
    }
  }
  if (opts.mode != MODE_NONE) {
    return corrupt(opts);
  }
}
