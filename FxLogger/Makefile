########################################
# Copyright (c) IBM Corp. 1997-2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
#
########################################

RPM_DIR := $(CURDIR)/../..

CXXFLAGS = -I. -IPk \
	-DPKTRACE_WORKING \
	-D NO_SETPRI \
	-D GFE_MPI \
	-g \
	-D PKFXLOG \
        -D PKTRACE_WORKING \
        -D PKTRACE_ON \
	-D _THREAD_SAFE=1 \
	-fpermissive \
	-DPK_BGP \
	-DPK_LINUX \
	-DPKFXLOG \
	-DPK_BGP \
	-DPK_LINUX \
	-g \
  -DONLY_MSPACES \


SRCS = FxLogger.cpp \
	Trace.cpp



CFLAGS=-I. -IPk \
	-DPKTRACE_WORKING \
	-D NO_SETPRI \
	-D GFE_MPI \
	-g \
	-D PKFXLOG \
        -D PKTRACE_WORKING \
        -D PKTRACE_ON \
	-D _THREAD_SAFE=1 \
	-DPK_BGP \
	-DPK_LINUX \
	-DPKFXLOG \
	-DPK_BGP \
	-DPK_LINUX \
	-g \
  -DONLY_MSPACES \


C_SRCS=dlmalloc.c


OBJS=$(SRCS:%.cpp=%.o) $(C_SRCS:%.c=%.o)

#CXX=$(CROSS_CXX)
#CC=$(CROSS_CC)
AR?=ar

libPkLinux.a: $(OBJS)
	$(AR) r $@ $^

install: libPkLinux.a
ifdef INSTALL_ROOTFS_DIR
	install -D -m 644 libPkLinux.a ${INSTALL_ROOTFS_DIR}/opt/fxlogger/lib
	install -D -m 644 Pk/FxLogger.hpp ${INSTALL_ROOTFS_DIR}/opt/fxlogger/include
	install -D -m 644 Pk/Trace.hpp ${INSTALL_ROOTFS_DIR}/opt/fxlogger/include
else
	echo "libPkLinux: Nothing to install."
endif

clean:
	$(RM) $(OBJS) $(SRCS:.cpp=.d) libPkLinux.a

# include Make.versions
-include ../../linux/Make.versions
ARCH=`/bin/arch`
.PHONY: rpm
rpm:
	rpmbuild -bb \
		--define "_topdir $(RPM_DIR)"  \
		--define "FXLOGGER_DIR $(CURDIR)" \
		--define "RPM_VERSION $(RPM_VERSION)" \
          ./fxlogger.spec
	mkdir -p $(RPM_DIR)/frontendnode-opt-RPMS && \
	cp $(RPM_DIR)/RPMS/$(ARCH)/bgas-fxlogger-$(RPM_VERSION)*.rpm $(RPM_DIR)/frontendnode-opt-RPMS

# dependency rules
%.d: %.c
	$(SHELL) -ec '$(CC) -M $(CFLAGS) $< | sed "s,$*.o,& $@,g" > $@'

%.d: %.cpp
	$(SHELL) -ec '$(CC) -M $(CXXFLAGS) $< | sed "s,$*.o,& $@,g" > $@'

# include dependency files
-include $(SRCS:.cpp=.d)
