#------------------------------------------------------------------------------
# Setup MPI variables for MVAPICH installation on RHEL 6.5 systems
#------------------------------------------------------------------------------
#
# Manually setup mpich variables
#  
set( MPICH_ROOT         /usr/lib64/mvapich2)
set( MPICH_INCLUDE_ROOT /usr/include/mvapich2-ppc64)
set( MPICH_LIB_PATH     /usr/lib64/mvapich2/lib)
set( MPI_FOUND 1)
set( MPI_INCLUDE_PATH     ${MPICH_INCLUDE_ROOT} CACHE STRING "" FORCE)
set( MPI_C_INCLUDE_PATH   ${MPICH_INCLUDE_ROOT} CACHE STRING "" FORCE)
set( MPI_CXX_INCLUDE_PATH ${MPICH_INCLUDE_ROOT} CACHE STRING "" FORCE)

# * * * * * * * * * * * *  *
# Warning : on BGAS, order of libs is very important, we must link 
# slurm;pmi;mpich ....
# * * * * * * * * * * * *  *

set(MPI_EXTRA_LIBRARY 
  ${MPICH_LIB_PATH}/libmpich.so;
  ${MPICH_LIB_PATH}/libopa.so;
  ${MPICH_LIB_PATH}/libmpl.so;
  rt;pthread CACHE STRING "" FORCE)

#
# Newer cmake FindMPI uses per language settings
#
set(MPI_C_LIBRARIES       ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)
set(MPI_CXX_LIBRARIES     ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)
set(MPI_LIBRARY           ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)

#################################################################
# Declare link directories for MPI/PAMI/SPI
#################################################################
set(MPI_LINK_DIRS
  ${MPICH_LIB_PATH}
  )
