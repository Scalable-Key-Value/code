########################################
#Copyright (c) IBM Corp. 2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
########################################
#skv top level makefile to BUILD AND INSTALL all components including test
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


SKV_FILES=skv.env skv skv_server.conf



.PHONY: install ${SKV_FILES}
all: install

build:
	@echo "nothing to be done for build in ${CURDIR}"


skv.env:
	${VV}cat etc/skv.env.in > ${SKV_BUILD_DIR}/$@

	install -D -m 644 ${SKV_BUILD_DIR}/$@ ${SKV_INSTALL_DIR}/etc/skv.env


skv:
	install -D -m 755 etc/init.d/skv ${SKV_INSTALL_DIR}/etc/init.d/skv 

skv_server.conf: etc/skv_server.conf
	install -D -m 644 etc/skv_server.conf ${SKV_INSTALL_DIR}/etc/skv_server.conf
	install -D -m 644 etc/skv_server.conf ${INSTALL_ROOTFS_DIR}/etc/skv_server.conf


install: ${SKV_FILES}




.PHONY: clean distclean
clean distclean:
	${VV}-rm -f ${SKV_BUILD_DIR}/skv.env
