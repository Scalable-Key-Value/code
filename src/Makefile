########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################
#skv makefile to BUILD AND INSTALL src components
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

# set the src dirs to traverse for make
# possible dirs are: common server client
SRC_DIRS=common client server


all: build


build install:
	@echo "$@ing skv sources"
	@set -e; for dest in ${SRC_DIRS}; do ${MAKE} ${DIR_VERBOSE} --directory $$dest $@; done


.PHONY: clean distclean
clean distclean:
	@set -e; for dest in ${SRC_DIRS}; do ${MAKE} ${DIR_VERBOSE} --directory $$dest $@; done

