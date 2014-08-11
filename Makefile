########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
########################################
# skv top level makefile to BUILD AND INSTALL all components including test
#
# Contributors:
#     arayshu, lschneid - initial implementation

ifndef SKV_TOP
    SKV_TOP=$(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export SKV_TOP
endif
# use skv/build as a central build dir for all skv components
export BUILD_DIR=$(SKV_TOP)/build

# picks up SKV-specific settings
# e.g.: SKV_DEST, SKV_BASE_DIR, SKV_GLOBAL_*FLAGS, ...
SKV_CONF?=rpm
export SKV_CONF
include ${SKV_TOP}/make.conf

RPM_DIR          := $(CURDIR)/..

ifndef SKV_MAKE_PROCESSES
	SKV_MAKE_PROCESSES=$(shell grep processor /proc/cpuinfo | wc -l)
	export SKV_MAKE_PROCESSES
endif


.PHONY: install build
all: build

install: build
	@echo "Target is: $@"
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest -j ${SKV_MAKE_PROCESSES} $@ INSTALL_ROOTFS_DIR=${INSTALL_ROOTFS_DIR}; \
	done
	install -D -m 644 README ${SKV_INSTALL_DIR}/doc/README
	install -D -m 644 CHANGELOG ${SKV_INSTALL_DIR}/doc/CHANGELOG

build: ${SKV_DEST} ${FXLOGGER_LIB}
	@if [ ! -d ${SKV_BUILD_DIR} ]; then echo "Creating ${SKV_BUILD_DIR}/mpi"; mkdir -p ${SKV_BUILD_DIR}/mpi; fi
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest -j ${SKV_MAKE_PROCESSES} $@; \
	done




.PHONY: clean distclean
clean:
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest $@; \
	done

distclean:
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest $@; \
	done
	-rm -f make.conf


${SKV_TOP}/make.conf:
	./bootstrap.sh

${FXLOGGER_DIR}/libPkLinux.a:
	$(MAKE) -C $(FXLOGGER_DIR)

ARCH=`/bin/arch`
.PHONY: rpm
rpm:
	rpmbuild -bb \
		--define "_topdir $(RPM_DIR)"  \
		--define "SKV_DIR $(CURDIR)" \
		--define "RPM_VERSION $(RPM_VERSION)" \
          ./skv.spec
	mkdir -p $(RPM_DIR)/ionode-opt-RPMS && \
	cp $(RPM_DIR)/RPMS/$(ARCH)/bgas-skv-$(RPM_VERSION)*.rpm $(RPM_DIR)/ionode-opt-RPMS
	mkdir -p $(RPM_DIR)/frontendnode-opt-RPMS && \
	cp $(RPM_DIR)/RPMS/$(ARCH)/bgas-skv-devel-$(RPM_VERSION)*.rpm $(RPM_DIR)/frontendnode-opt-RPMS
