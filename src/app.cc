#include <stdio.h>
#include <stdlib.h>

#include "layer1.h"

void l2_outgoing(struct l1_global *l1, struct l1_socket *ls) {
  printf("l2_outgoing\n");
  l1_send(l1, ls, "hello", 5, 0, 0);
}

void l2_deliver(struct l1_global *l1, struct l1_socket *ls,
    const char *data, int size, void (*release)(void *), void *arg) {
  printf("l1_deliver: got %d bytes: '%.*s'\n", size, size, data);
  (*release)(arg);
}

int main(int argc, char **argv) {
  int inport = atoi(argv[1]);
  int outport = atoi(argv[2]);

  printf("hello world\n");

  struct l1_global *l1;
  l1 = l1_init(0, l2_outgoing, l2_deliver);
  l1_listen(l1, inport);

  l1_connect(l1, "localhost", outport);

  for (;;) {
    printf("start waiting\n");
    l1_wait(l1, 10000);
  }
  return 0;
}
