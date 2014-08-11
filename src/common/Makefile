########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################
#skv makefile to BUILD AND INSTALL common components library
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


SRC_CPP=skv_utils.cpp \
	skv_distribution_manager.cpp \
	skv_config.cpp \


MAIN_TARGET=libskv_common.a
SKV_CXXFLAGS=-O2 -g -I${SKV_INCLUDE_DIR} ${SKV_GLOBAL_CXXFLAGS}


OBJ=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.o)
DEP=$(SRC_CPP:%.cpp=${SKV_BUILD_DIR}/%.d)

all: build


${MAIN_TARGET}: ${OBJ}
	@${AR} r ${SKV_BUILD_DIR}/$@ $^



prebuild:
	${VV}if [ ! -e ${SKV_BUILD_DIR} ]; then \
		echo "Creating build dir: ${SKV_BUILD_DIR}"; \
		mkdir -p ${SKV_BUILD_DIR}; \
	fi


build: prebuild ${OBJ} ${MAIN_TARGET}
	@echo "${CURDIR}: Finished build..."


install: build
	@echo "${CURDIR}: Starting install now..."
	install -D -m 644 ${SKV_BUILD_DIR}/${MAIN_TARGET} ${SKV_INSTALL_DIR}/lib/${MAIN_TARGET}


${SKV_BUILD_DIR}/%.o: %.cpp
	@echo "compiling $<"
	${VV}$(CXX) -c $(SKV_CXXFLAGS) -DSKV_CLIENT_UNI $< -o ${SKV_BUILD_DIR}/$*.o




.PHONY: clean distclean
clean distclean:
	${VV}-rm ${OBJ} ${DEP} ${SKV_BUILD_DIR}/${MAIN_TARGET}





# rule to create automatic dependency list for all files
${SKV_BUILD_DIR}/%.d: %.cpp
	@$(SHELL) -ec '$(CXX) -MM $(SKV_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${SKV_BUILD_DIR}/\1.o $@ : #g" > $@'


-include ${DEP}
