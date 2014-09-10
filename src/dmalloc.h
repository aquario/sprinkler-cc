#ifndef DMALLOC_H_
#define DMALLOC_H_

#include <stdlib.h>

// #define MALLOC_DEBUG

#ifdef MALLOC_DEBUG

#define dmalloc(s)		dm_malloc(s, __FILE__, __LINE__)
#define dcalloc(n, s)	dm_calloc(n, s, __FILE__, __LINE__)
#define drealloc(d, s)	dm_realloc(d, s, __FILE__, __LINE__)
#define dfree(d)		dm_free(d, __FILE__, __LINE__)

void *dm_malloc(int size, char *file, int line);
void *dm_calloc(int n, int size, char *file, int line);
void *dm_realloc(void *data, int size, char *file, int line);
void dm_free(void *data, char *file, int line);

#else // MALLOC_DEBUG

#ifdef PROFILING

void *do_malloc(int size);
void *do_calloc(int n, int size);
void *do_realloc(void *data, int size);
void do_free(void *data);

#define dmalloc(s)		do_malloc(s)
#define dcalloc(n, s)	do_calloc(n, s)
#define drealloc(d, s)	do_realloc(d, s)
#define dfree(d)		do_free(d)

#else // PROFILING

#define dmalloc(s)		malloc(s)
#define dcalloc(n, s)	calloc(n, s)
#define drealloc(d, s)	realloc(d, s)
#define dfree(d)		free(d)

#endif  // PROFILING

#endif  // MALLOC_DEBUG

#endif  // DMALLOC_H_
