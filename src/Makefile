CXX		= g++
CXXFLAGS	= -g -std=c++0x -O2 -lglog -lgflags

proxy: proxy.cc sprinkler_node.o multi_tier_storage.o transport_layer.o sprinkler_common.o dmalloc.o
	$(CXX) $(CXXFLAGS) -o proxy proxy.cc sprinkler_node.o multi_tier_storage.o transport_layer.o sprinkler_common.o dmalloc.o

transport_layer_test: transport_layer_test.o transport_layer.o dmalloc.o
	$(CXX) $(CXXFLAGS) -o transport_layer_test transport_layer_test.o transport_layer.o dmalloc.o

multi_tier_storage.o: multi_tier_storage.h sprinkler_common.o dmalloc.o
transport_layer_test.o: transport_layer.h dmalloc.h
transport_layer.o: transport_layer.h dmalloc.h
sprinkler_common.o: sprinkler_common.h
dmalloc.o: dmalloc.h

clean:
	rm -rf *.o *_test proxy *.dSYM/
