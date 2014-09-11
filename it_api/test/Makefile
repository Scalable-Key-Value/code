########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
########################################
#
# Contributors:
#     arayshu, lschneid - initial implementation

#set default TOP-level dir not set yet
ifndef ITAPE_TOP
    ITAPE_TOP=$(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export ITAPE_TOP
endif

# picks up IT-API-specific settings
# e.g.: IT_API_DEST, IT_API_BASE_DIR, IT_API_GLOBAL_*FLAGS, ...
include $(ITAPE_TOP)/make.conf


SRC_CPP=it_skv_comm_test.cpp
#it_api_test.cpp
TESTS=$(SRC_CPP:%.cpp=%)

MPI_DIR?=$(ITAPE_TOP)/../../ramdisk/modules/${SKV_MPI_IMPL}/build/opt/${SKV_MPI_IMPL}
MPICXX=${MPI_DIR}/bin/mpicxx

IT_API_CXXFLAGS=-g -I${IT_API_INCLUDE_DIR} ${IT_API_GLOBAL_CXXFLAGS} -I${MPI_DIR}/include
ifeq (${IT_API_IMPL}, sockets)
IT_API_LDFLAGS=-O2 -g ${BUILD_DIR}/libit_api_o_${IT_API_IMPL}.a ${IT_API_GLOBAL_LDFLAGS}  -lpthread -L${MPI_DIR}/lib
else
IT_API_LDFLAGS=-O2 -g ${BUILD_DIR}/libit_api_o_${IT_API_IMPL}.a ${IT_API_GLOBAL_LDFLAGS}  -lpthread -lrdmacm -L${MPI_DIR}/lib
endif

all: build


###############################################
# add your link targets here to determine object and library
# dependencies as well as specifying link procedures

it_api_test: ${BUILD_DIR}/it_api_test.o ${BUILD_DIR}/libit_api_o_${IT_API_IMPL}.a
	$(CXX) $< ${IT_API_LDFLAGS} -o $@

it_skv_comm_test: ${BUILD_DIR}/it_skv_comm_test.o ${BUILD_DIR}/libit_api_o_${IT_API_IMPL}.a
	$(CXX) $< ${IT_API_LDFLAGS} -lrt -o $@


###############################################
# no customization required below this line

OBJ=$(SRC_CPP:%.cpp=${BUILD_DIR}/%.o)
DEP=$(SRC_CPP:%.cpp=${BUILD_DIR}/%.d)


build: ${OBJ} ${TESTS}
	@echo "Finished common build..."


install: build
	@echo "Starting install now..."

ifeq (${SKV_TARGET}, linux)
${BUILD_DIR}/%.o: %.cpp
	$(MPICXX) -c $(IT_API_CXXFLAGS) $< -o ${BUILD_DIR}/$*.o
else
${BUILD_DIR}/%.o: %.cpp
	$(CXX) -c $(IT_API_CXXFLAGS) $< -o ${BUILD_DIR}/$*.o
endif



.PHONY: clean distclean
clean distclean:
	-rm ${OBJ} ${DEP} ${TESTS}





# rule to create automatic dependency list for all files
ifeq (${SKV_TARGET}, linux)
${BUILD_DIR}/%.d: %.cpp
	$(SHELL) -ec '$(MPICXX) -MM $(IT_API_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${BUILD_DIR}/\1.o $@ : #g" > $@'
else
${BUILD_DIR}/%.d: %.cpp
	$(SHELL) -ec '$(CXX) -MM $(IT_API_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${BUILD_DIR}/\1.o $@ : #g" > $@'
endif

-include ${DEP}
