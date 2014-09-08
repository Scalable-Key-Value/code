########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################
#skv makefile to INSTALL header files
#
#Contributors:
#    lschneid - initial implementation

ifndef SKV_TOP
    SKV_TOP=$(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export SKV_TOP
endif

# picks up SKV-specific settings
# e.g.: SKV_DEST, SKV_BASE_DIR, SKV_BUILD_DIR, SKV_GLOBAL_*FLAGS, ...
include ${SKV_TOP}/make.conf


SKV_HEADERS=client/skv_client.hpp \
  common/skv_types_ext.hpp \
  common/skv_errno.hpp \


.PHONY: install

all:

build:
	@echo "Nothing to build here..."

install: ${SKV_HEADERS}
	@echo "Installing header files..."
	for file in ${SKV_HEADERS}; do \
	    install -v -D -m 644 $$file ${SKV_INSTALL_DIR}/include/$$file; \
	done


clean distclean:
	@echo "Nothing to clean..."
