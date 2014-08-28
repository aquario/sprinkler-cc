CXX		= g++
CXXFLAGS	= -g -std=c++0x -O2 -lglog -lgflags

transport_layer_test: transport_layer_test.o transport_layer.o dmalloc.o
	$(CXX) $(CXXFLAGS) -o transport_layer_test transport_layer_test.o transport_layer.o dmalloc.o

transport_layer_test.o:		transport_layer.h dmalloc.h
transport_layer.o:	transport_layer.h dmalloc.h
dmalloc.o:	dmalloc.h

clean:
	rm *.o *_test
