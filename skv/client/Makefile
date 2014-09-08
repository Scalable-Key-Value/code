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
include ${SKV_TOP}/make.conf


SRC_CPP=skv_client.cpp \
	skv_client_internal.cpp \
	skv_client_conn_manager_if.cpp \
	skv_client_cursor.cpp \
	skv_client_command_manager_if.cpp \
	skv_client_bulk_inserter.cpp \
	skv_client_2_server_active_bcast.cpp \


UNI_TARGET=libskv_client.a
MPI_TARGET=libskv_client_mpi.a
SKV_CXXFLAGS=-O2 -g ${ASF_GLOBAL_CXXFLAGS} -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CXXFLAGS}


UNI_OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.o)
MPI_OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/mpi/%.o)
DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.d)


all: build
	echo "standard build complete"

${UNI_TARGET}: ${UNI_OBJ}
	@${AR} r ${SKV_BUILD_DIR}/$@ $^

${MPI_TARGET}: ${MPI_OBJ}
	@${AR} r ${SKV_BUILD_DIR}/$@ $^



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
	install -D -m 644 ${SKV_BUILD_DIR}/${MPI_TARGET} ${SKV_INSTALL_DIR}/lib/${MPI_TARGET}
	install -D -m 644 ${SKV_BUILD_DIR}/${UNI_TARGET} ${SKV_INSTALL_DIR}/lib/${UNI_TARGET}


${SKV_BUILD_DIR}/mpi/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(MPICXX) -c $(SKV_CXXFLAGS) $< -o ${SKV_BUILD_DIR}/mpi/$*.o

${SKV_BUILD_DIR}/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(MPICXX) -c $(SKV_CXXFLAGS) -DSKV_CLIENT_UNI $< -o ${SKV_BUILD_DIR}/$*.o




.PHONY: clean distclean
clean distclean:
	${VV}-rm ${MPI_OBJ} ${UNI_OBJ} ${DEP} ${SKV_BUILD_DIR}/${UNI_TARGET} ${SKV_BUILD_DIR}/${MPI_TARGET}





# rule to create automatic dependency list for all files
${SKV_BUILD_DIR}/%.d: %.cpp
	@$(SHELL) -ec '$(MPICXX) -MM $(SKV_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'


-include ${DEP}
