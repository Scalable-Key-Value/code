########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################
#skv makefile to BUILD AND INSTALL client library
#
#Contributors:
#    arayshu, lschneid - initial implementation

ifndef SKV_TOP
    SKV_TOP=$(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export SKV_TOP
endif

# picks up SKV-specific settings
# e.g.: SKV_DEST, SKV_BASE_DIR, SKV_BUILD_DIR, SKV_GLOBAL_*FLAGS, ...
CONF_SOURCED=no
include ${SKV_TOP}/make.conf


SRC_CPP=skv.cpp \

UNI_TARGET=libskvc.a
MPI_TARGET=libskvc_mpi.a
SKV_CXXFLAGS=-O2 -g -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CXXFLAGS} -DSKV_PURE
SKV_CFLAGS=-O2 -g -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CFLAGS} -DSKV_PURE

UNI_OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.o)
MPI_OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/mpi/%.o)
UNI_DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.d)
MPI_DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/mpi/%.d)

IT_API_OBJ=it_api_o_verbs.o it_api_o_verbs_thread.o
PATHED_IT_OBJ=$(IT_API_OBJ:%.o=${SKV_TOP}/build/%.o)

COMMON_OBJ=skv_utils.o skv_distribution_manager.o skv_config.o
PATHED_COMMON_OBJ=$(COMMON_OBJ:%.o=${SKV_BUILD_DIR}/%.o)

CLIENT_OBJ=skv_client.o \
	skv_client_internal.o \
	skv_client_conn_manager_if.o \
	skv_client_cursor.o \
	skv_client_command_manager_if.o \
	skv_client_bulk_inserter.o \
	skv_client_2_server_active_bcast.o

PATHED_CLIENT_OBJ=$(CLIENT_OBJ:%.o=${SKV_BUILD_DIR}/%.o)
PATHED_CLIENT_MPI_OBJ=$(CLIENT_OBJ:%.o=${SKV_BUILD_DIR}/mpi/%.o)

ALL_CLIENT_OBJ=${PATHED_IT_OBJ} ${PATHED_COMMON_OBJ} ${PATHED_CLIENT_OBJ}
ALL_CLIENT_MPI_OBJ=${PATHED_IT_OBJ} ${PATHED_COMMON_OBJ} ${PATHED_CLIENT_MPI_OBJ}

RANLIB=ranlib

# pull in definitions for file-based client
ifeq (${SKV_INCLUDE_FILEBASED},yes)
include filebased.mk
else

all: build
	echo "standard build complete"

endif


${UNI_TARGET}: ${UNI_OBJ} ${ALL_CLIENT_OBJ}
	${VV}${AR} r ${SKV_BUILD_DIR}/$@ $^
	${VV}${RANLIB} ${SKV_BUILD_DIR}/$@


${MPI_TARGET}: ${MPI_OBJ} ${ALL_CLIENT_MPI_OBJ}
	${VV}${AR} r ${SKV_BUILD_DIR}/$@ $^
	${VV}${RANLIB} ${SKV_BUILD_DIR}/$@



prebuild:
	@if [ ! -e ${SKV_BUILD_DIR} ]; then \
		echo "Creating build dir: ${SKV_BUILD_DIR}"; \
		mkdir -p ${SKV_BUILD_DIR}; \
	fi
	@if [ ! -e ${SKV_BUILD_DIR}/mpi ]; then \
		echo "Creating build dir: ${SKV_BUILD_DIR}/mpi"; \
		mkdir -p ${SKV_BUILD_DIR}/mpi; \
	fi


build_mpi: ${MPI_OBJ} ${MPI_TARGET}
	@echo "${CURDIR}: Finished MPI build..."

build_uni: ${UNI_OBJ} ${UNI_TARGET}
	@echo "${CURDIR}: Finished single-node build..."

.PHONY: build
build: prebuild build_mpi build_uni



install: build
	@echo "${CURDIR}: Starting install now..."


${SKV_BUILD_DIR}/mpi/%.o: %.c
	@echo "compiling $<"
	${VV}$(MPICXX) -c $(SKV_CFLAGS) $< -o ${SKV_BUILD_DIR}/mpi/$*.o

${SKV_BUILD_DIR}/%.o: %.c
	@echo "compiling $<"
	${VV}$(CXX) -c $(SKV_CFLAGS) -DSKV_CLIENT_UNI $< -o ${SKV_BUILD_DIR}/$*.o

${SKV_BUILD_DIR}/mpi/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(MPICXX) -c $(SKV_CXXFLAGS) $< -o ${SKV_BUILD_DIR}/mpi/$*.o

${SKV_BUILD_DIR}/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(CXX) -c $(SKV_CXXFLAGS) -DSKV_NON_MPI -DSKV_CLIENT_UNI $< -o ${SKV_BUILD_DIR}/$*.o




.PHONY: clean distclean
clean distclean: ${FILEBASEDCLEAN}
	${VV}-rm ${MPI_OBJ} ${UNI_OBJ} ${UNI_DEP} ${MPI_DEP} ${SKV_BUILD_DIR}/${UNI_TARGET} ${SKV_BUILD_DIR}/${MPI_TARGET}





# rule to create automatic dependency list for all files
${SKV_BUILD_DIR}/mpi/%.d: %.cpp
	@$(SHELL) -ec '$(MPICXX) -MM $(SKV_CXXFLAGS) $< \
                          | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/mpi/\1.o $@ : #g" > $@'

${SKV_BUILD_DIR}/%.d: %.cpp
	@$(SHELL) -ec '$(CXX) -MM -DSKV_CLIENT_UNI -DSKV_NON_MPI $(SKV_CXXFLAGS) $< \
                          | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'


include ${UNI_DEP}
include ${MPI_DEP}
