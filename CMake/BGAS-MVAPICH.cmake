#------------------------------------------------------------------------------
# Setup MPI variables for BGAS compilation using clang
# Set BGAS to 1 for safety before including this file 
#------------------------------------------------------------------------------
if(BGAS)

  set(SLURM_LIBRARY_PATH /bgsys/bgas/opt/slurm/14.03.3.2/lib )
  
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
    ${SLURM_LIBRARY_PATH}/libslurm.so;
    ${SLURM_LIBRARY_PATH}/libpmi.so;
    ${MPICH_LIB_PATH}/libmpich.so;
    ${MPICH_LIB_PATH}/libopa.so;
    ${MPICH_LIB_PATH}/libmpl.so;
    rt;pthread CACHE STRING "" FORCE)

  #
  # Newer cmake FindMPI uses per language settings
  #
  set(MPI_C_LIBRARIES       ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)
  set(MPI_CXX_LIBRARIES     ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)
  set(MPI_LIBRARY           ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE )

  #################################################################
  # Declare link directories for MPI/PAMI/SPI
  #################################################################
  set(MPI_LINK_DIRS
    ${SLURM_LIBRARY_PATH}
    ${MPICH_LIB_PATH}
  )
	# don't need to call this, but do it anyway just to be sure everything is initialized
  #find_package(MPI REQUIRED)
endif()
