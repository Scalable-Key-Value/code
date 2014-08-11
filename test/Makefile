########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################

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


SRC_C=skv_test_clib.c \


SRC_CPP=skv_test_insert_retrieve_sync.cpp \
	skv_test_insert_retrieve_async.cpp \
	test_skv_insert_command.cpp \
	test_skv_remove_command.cpp \
	skv_test_bulk.cpp \
	skv_base_test.cpp \

ifeq ( $(SKV_LOCAL_KV_BACKEND), rocksdb )
  SRC_CPP+=rocksdb_test.cpp
endif


SKV_CXXFLAGS=-O2 -g -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CXXFLAGS} #-DSKV_CLIENT_UNI
SKV_CFLAGS=${SKV_CXXFLAGS} ${SKV_GLOBAL_CFLAGS} -DSKV_PURE
SKV_LDFLAGS=-O2 -g ${SKV_GLOBAL_LDFLAGS}
MPICLIENT_LIBS=${SKV_BUILD_DIR}/libskv_client_mpi.a ${SKV_BUILD_DIR}/libskv_common.a
UNICLIENT_LIBS=${SKV_BUILD_DIR}/libskv_client.a ${SKV_BUILD_DIR}/libskv_common.a
SKV_MPI_CLIBS=${SKV_BUILD_DIR}/libskvc_mpi.a -lm ${SKV_GLOBAL_LDFLAGS}
SKV_UNI_CLIBS=${SKV_BUILD_DIR}/libskvc.a -lm ${SKV_GLOBAL_LDFLAGS}


#includes_test.cpp test_errno.cpp test_skv_utils.cpp
TESTS=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%) $(SRC_C:%.c=${SKV_BUILD_DIR}/%)

all: build


###############################################
# add your link targets here to determine object and library
# dependencies as well as specifying link procedures

${SKV_BUILD_DIR}/skv_test_insert_retrieve_sync: ${SKV_BUILD_DIR}/skv_test_insert_retrieve_sync.o
	@echo "Building $@"
	${VV}$(MPICXX) $< ${MPICLIENT_LIBS} ${SKV_LDFLAGS} -o $@

${SKV_BUILD_DIR}/skv_test_insert_retrieve_async: ${SKV_BUILD_DIR}/skv_test_insert_retrieve_async.o
	@echo "Building $@"
	${VV}$(MPICXX) $< ${MPICLIENT_LIBS} ${SKV_LDFLAGS} -o $@

${SKV_BUILD_DIR}/skv_test_bulk: ${SKV_BUILD_DIR}/skv_test_bulk.o
	@echo "Building $@"
	${VV}$(MPICXX) $< ${MPICLIENT_LIBS} ${SKV_LDFLAGS} -o $@

${SKV_BUILD_DIR}/test_skv_insert_command: ${SKV_BUILD_DIR}/test_skv_insert_command.o
	@echo "Building $@"
	${VV}$(MPICXX) $< ${UNICLIENT_LIBS} ${SKV_LDFLAGS} -o $@

${SKV_BUILD_DIR}/skv_base_test: ${SKV_BUILD_DIR}/skv_base_test.o 
	@echo "Building skv_base_test"
	${VV}$(CXX) $< ${UNICLIENT_LIBS} ${SKV_LDFLAGS} -o $@

${SKV_BUILD_DIR}/test_skv_remove_command: ${SKV_BUILD_DIR}/test_skv_remove_command.o
	@echo "Building $@"
	${VV}$(MPICXX) $< ${MPICLIENT_LIBS} ${SKV_LDFLAGS} -o $@

${SKV_BUILD_DIR}/skv_test_clib: ${SKV_BUILD_DIR}/skv_test_clib.o
	@echo "Building skv_size_test"
	${VV}$(MPICXX) $< ${SKV_MPI_CLIBS} ${CLIB_LDFLAGS} -o $@

${SKV_BUILD_DIR}/rocksdb_test: ${SKV_BUILD_DIR}/rocksdb_test.o
	@echo "Building $@"
	${VV}$(CXX) $< ${SKV_LDFLAGS} -o $@

###############################################
# no customization should be required below this line

OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.o) $(SRC_C:%.c=${SKV_BUILD_DIR}/%.o)
DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.d) $(SRC_C:%.c=${SKV_BUILD_DIR}/%.d)


prebuild:
	@if [ ! -e ${SKV_BUILD_DIR} ]; then \
		echo "Creating build dir: ${SKV_BUILD_DIR}"; \
		mkdir -p ${SKV_BUILD_DIR}; \
	fi
#	@${MAKE} --directory ../src build


build: prebuild ${OBJ} ${TESTS}
	@echo "Finished common build..."


install: build
	@echo "Starting install now..."
	install -D -m 755 ${SKV_BUILD_DIR}/skv_test_insert_retrieve_sync ${SKV_INSTALL_DIR}/bin/skv_test_insert_retrieve_sync
	install -D -m 755 ${SKV_BUILD_DIR}/skv_test_insert_retrieve_async ${SKV_INSTALL_DIR}/bin/skv_test_insert_retrieve_async
	install -D -m 755 ${SKV_BUILD_DIR}/test_skv_remove_command ${SKV_INSTALL_DIR}/bin/test_skv_remove_command
	install -D -m 755 ${SKV_BUILD_DIR}/test_skv_insert_command ${SKV_INSTALL_DIR}/bin/test_skv_insert_command
	install -D -m 755 ${SKV_BUILD_DIR}/skv_test_bulk ${SKV_INSTALL_DIR}/bin/skv_test_bulk
	install -D -m 755 ${SKV_BUILD_DIR}/skv_base_test ${SKV_INSTALL_DIR}/bin/skv_base_test


${SKV_BUILD_DIR}/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(MPICXX) -c $(SKV_CXXFLAGS) $< -o ${SKV_BUILD_DIR}/$*.o

${SKV_BUILD_DIR}/%.o: %.c
	@echo "compiling $<"
	${VV}$(MPICC) -c $(SKV_CFLAGS) $< -o ${SKV_BUILD_DIR}/$*.o


.PHONY: clean distclean
clean distclean:
	${VV}-rm ${OBJ} ${DEP} ${TESTS}





# rule to create automatic dependency list for all files
${SKV_BUILD_DIR}/%.d: %.cpp
	@$(SHELL) -ec '$(CXX) -MM $(SKV_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'

${SKV_BUILD_DIR}/%.d: %.c
	@$(SHELL) -ec '$(CC) -MM $(SKV_CFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'


-include ${DEP}
