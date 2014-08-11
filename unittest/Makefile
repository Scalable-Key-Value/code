########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################
#skv top level makefile to BUILD AND INSTALL server components
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

SRC_CPP=\
	test_skv_ringbuffer_ptr.cpp  \
	test_skv_rdma_data_buffer.cpp


OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.o)
DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.d)
TARGETS=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%)

SKV_CXXFLAGS=-O2 -g -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CXXFLAGS} -Wall

all: ${TARGETS}

prebuild:
	@if [ ! -e ${SKV_BUILD_DIR} ]; then \
		echo "Creating build dir: ${SKV_BUILD_DIR}"; \
		mkdir -p ${SKV_BUILD_DIR}; \
	fi

build: prebuild ${OBJ} ${TARGETS}
	@echo "${CURDIR}: Finished build..."

install: build
	@echo "${CURDIR}: Starting install now..."
	install -D -m 755 ${SKV_BUILD_DIR}/test_skv_ringbuffer_ptr ${SKV_INSTALL_DIR}/bin/test_skv_ringbuffer_ptr
	install -D -m 755 ${SKV_BUILD_DIR}/test_skv_rdma_data_buffer ${SKV_INSTALL_DIR}/bin/test_skv_rdma_data_buffer


%: %.o
	${VV}${CXX} $< ${SKV_GLOBAL_LDFLAGS} -o $@

${SKV_BUILD_DIR}/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(CXX) -c $(SKV_CXXFLAGS) $< -o ${SKV_BUILD_DIR}/$*.o


.PHONY: clean distclean
clean distclean:
	${VV}-rm ${OBJ} ${DEP} ${TARGETS}



# rule to create automatic dependency list for all files
${SKV_BUILD_DIR}/%.d: %.cpp
	@$(SHELL) -ec '$(CXX) -MM $(SKV_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'


-include ${DEP}


