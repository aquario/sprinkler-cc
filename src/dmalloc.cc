#include <stdio.h>
#include <stdlib.h>

#include "dmalloc.h"

#ifdef PROFILING

void *do_malloc(int size) {
  return malloc(size);
}

void *do_calloc(int n, int size) {
  return calloc(n, size);
}

void *do_realloc(void *data, int size) {
  return realloc(data, size);
}

void do_free(void *data) {
  return free(data);
}

#else // PROFILING

#define MAGIC 0x12345678

struct malloc_header {
  struct malloc_header *next, **back;
  unsigned int magic;
  int size;
  char *file;
  int line;
};

struct malloc_header *dm_list;

void dm_dump() {
  struct malloc_header *mh;

  for (mh = dm_list; mh != 0; mh = mh->next) {
    printf("%s:%d: %d\n", mh->file, mh->line, mh->size);
    if (mh->magic != MAGIC) {
      fprintf(stderr, "dm_dump: magic number corrupted\n");
    }
  }
}

void *dm_malloc(int size, char *file, int line) {
  struct malloc_header *mh =
      (struct malloc_header *) malloc(sizeof(*mh) + size);

  mh->magic = MAGIC;
  mh->size = size;
  mh->file = file;
  mh->line = line;
  if ((mh->next = dm_list) != 0) {
    dm_list->back = &mh->next;
  }
  mh->back = &dm_list;
  dm_list = mh;

  char *p = (char *) &mh[1];
  while (--size >= 0) {
    *p++ = 0xFF;
  }

  return &mh[1];
}

void *dm_calloc(int n, int size, char *file, int line) {
  struct malloc_header *mh =
      (struct malloc_header *) calloc(1, sizeof(*mh) + (n * size));

  mh->magic = MAGIC;
  mh->size = n * size;
  mh->file = file;
  mh->line = line;
  if ((mh->next = dm_list) != 0) {
    dm_list->back = &mh->next;
  }
  mh->back = &dm_list;
  dm_list = mh;
  return &mh[1];
}

void *dm_realloc(void *data, int size, char *file, int line) {
  struct malloc_header *mh = &((struct malloc_header *) data)[-1];

  if (mh->magic != MAGIC) {
    fprintf(stderr, "dm_realloc: magic number corrupted\n");
    exit(1);
  }
  mh = (struct malloc_header *) realloc(mh, sizeof(*mh) + size);
  mh->file = file;
  mh->line = line;
  return &mh[1];
}

void dm_print(void *data) {
  struct malloc_header *mh = &((struct malloc_header *) data)[-1];

  if (mh->magic != MAGIC) {
    fprintf(stderr, "dm_printf: magic number corrupted\n");
    exit(1);
  }
  printf("dm_print: %s %d %d\n", mh->file, mh->line, mh->size);
}

void dm_free(void *data, char *file, int line) {
  struct malloc_header *mh = &((struct malloc_header *) data)[-1];
  int size = sizeof(*mh) + mh->size;

  if (mh->magic != MAGIC) {
    fprintf(stderr, "dm_free: magic number corrupted\n");
    exit(1);
  }
  if ((*mh->back = mh->next) != 0) {
    mh->next->back = mh->back;
  }

  char *p = (char *) data;
  while (--size > 0) {
    *p = 0xFF;
  }

  free(mh);
}

#endif // PROFILING
