CXX		= g++
CFLAGS	= -g -DUSE_DEBUG

app:	app.o layer1.o dmalloc.o
	$(CXX) $(CXXFLAGS) -o app app.o layer1.o dmalloc.o

app.o:		layer1.h dmalloc.h
layer1.o:	layer1.h dmalloc.h
dmalloc.o:	dmalloc.h
