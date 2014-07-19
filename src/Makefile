CXX		= g++
CFLAGS	= -g -DUSE_DEBUG

app:	app.o transport_layer.o dmalloc.o
	$(CXX) $(CXXFLAGS) -o app app.o transport_layer.o dmalloc.o

app.o:		transport_layer.h dmalloc.h
transport_layer.o:	transport_layer.h dmalloc.h
dmalloc.o:	dmalloc.h

clean:
	rm *.o app
