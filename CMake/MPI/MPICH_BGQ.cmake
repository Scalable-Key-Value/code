#
# Manually setup MPI variables for static linking on BGQ
#  
set( CNK_BASE             /bgsys/drivers/ppcfloor)
set( CNK_MPI_INCLUDES     ${CNK_BASE}/comm/lib/gnu  ${CNK_BASE} ${CNK_BASE}/comm/sys/include ${CNK_BASE}/spi/include ${CNK_BASE}/spi/include/kernel/cnk)
set( CNK_MPI_LIB_ROOT     ${CNK_BASE}/comm/lib )
set( CNK_MPI_LIBPATH      ${CNK_BASE}/comm/lib ${CNK_BASE} ${CNK_BASE}/comm/lib64 ${CNK_BASE}/spi/lib ${CNK_BASE}/comm/sys/lib)

set( MPI_FOUND 1)
set( MPI_INCLUDE_PATH     ${CNK_MPI_INCLUDES} CACHE STRING "" FORCE)
set( MPI_C_INCLUDE_PATH   ${CNK_MPI_INCLUDES} CACHE STRING "" FORCE)
set( MPI_CXX_INCLUDE_PATH ${CNK_MPI_INCLUDES} CACHE STRING "" FORCE)

#
# List of libraries collected from mpicxx wrapper
#
set( MPI_EXTRA_LIBRARY 
  ${CNK_MPI_LIB_ROOT}/libmpich-gcc.a;
  ${CNK_MPI_LIB_ROOT}/libopa-gcc.a;
  ${CNK_MPI_LIB_ROOT}/libmpl-gcc.a;
  ${CNK_MPI_LIB_ROOT}/libpami-gcc.a;
  ${CNK_BASE}/spi/lib/libSPI.a
  ${CNK_BASE}/spi/lib/libSPI_cnk.a
  rt;pthread CACHE STRING "" FORCE )

#
# Newer cmake FindMPI uses per language settings
#
set( MPI_C_LIBRARIES       ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE )
set( MPI_CXX_LIBRARIES     ${CNK_MPI_LIB_ROOT}/libmpichcxx-gcc.a ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE )
set( MPI_LIBRARY           ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE )
