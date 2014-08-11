########################################
# Copyright (c) IBM Corp. 2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
########################################
#
# Contributors:
#     arayshu, lschneid - initial implementation

ifndef ITAPE_TOP
    ITAPE_TOP=$(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export ITAPE_TOP
endif

include $(ITAPE_TOP)/make.conf


BUILD=${IT_API_DEST}

.PHONY: build install

all: build

build:
	@echo "BUILDING TARGETS: ${BUILD}"
	@set -e; for i in $(BUILD); do \
		make -C $$i; \
	done

install:
	@set -e; for i in $(BUILD); do \
		make -C $$i install; \
	done



.PHONY: clean distclean
clean:
	@set -e; for subdest in $(BUILD); do \
		${MAKE} --directory $$subdest $@; \
	done

distclean:
	@set -e; for subdest in $(BUILD); do \
		${MAKE} --directory $$subdest $@; \
	done
	-rm -f make.conf


$(ITAPE_TOP)/make.conf:
	./bootstrap.sh
