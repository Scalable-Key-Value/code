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


SRC_CPP= \
	skv_server_uber_pds.cpp \
	skv_server_tree_based_container.cpp \
	skv_server_network_event_manager.cpp \
	skv_server_IT_event_source.cpp \
	skv_server_command_event_source.cpp \
	skv_server_local_kv_event_source.cpp \
	skv_local_kv_$(SKV_LOCAL_KV_BACKEND).cpp \
	skv_server.cpp \
	skv_server_main.cpp \



MAIN_TARGET=SKVServer
SKV_CXXFLAGS=-O2 -g -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CXXFLAGS}


OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.o)
DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.d)
COMMON_DEPS=${SKV_BUILD_DIR}/libskv_common.a \


vpath ${MAIN_TARGET} ${SKV_BUILD_DIR}


.PHONY: all build prebuild install
all: build



${SKV_BUILD_DIR}/${MAIN_TARGET}: ${OBJ} ${COMMON_DEPS}
	@echo "linking $@"
	${VV}${MPICXX} -O2 $^ ${SKV_GLOBAL_LDFLAGS} -o $@



prebuild:
	@if [ ! -e ${SKV_BUILD_DIR} ]; then \
		echo "Creating build dir: ${SKV_BUILD_DIR}"; \
		mkdir -p ${SKV_BUILD_DIR}; \
	fi


build: prebuild ${OBJ} ${SKV_BUILD_DIR}/${MAIN_TARGET}
	@echo "${CURDIR}: Finished build..."


install: build
	@echo "${CURDIR}: Starting install now..."
	install -D -m 755 ${SKV_BUILD_DIR}/${MAIN_TARGET} ${SKV_INSTALL_DIR}/bin/${MAIN_TARGET}


${SKV_BUILD_DIR}/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(MPICXX) -c $(SKV_CXXFLAGS) $< -o ${SKV_BUILD_DIR}/$*.o




.PHONY: clean distclean
clean distclean:
	${VV}-rm ${OBJ} ${DEP} ${SKV_BUILD_DIR}/${MAIN_TARGET}





# rule to create automatic dependency list for all files
${SKV_BUILD_DIR}/%.d: %.cpp
	@$(SHELL) -ec '$(CXX) -MM $(SKV_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'


-include ${DEP}


