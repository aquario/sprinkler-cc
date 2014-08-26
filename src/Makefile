CXX		= g++
CXXFLAGS	= -g -lglog -lgflags -DUSE_DEBUG

transport_layer_test: transport_layer_test.o transport_layer.o dmalloc.o
	$(CXX) $(CXXFLAGS) -o tl_test transport_layer_test.o transport_layer.o dmalloc.o

tl_test.o:		transport_layer.h dmalloc.h
transport_layer.o:	transport_layer.h dmalloc.h
dmalloc.o:	dmalloc.h

clean:
	rm *.o app
