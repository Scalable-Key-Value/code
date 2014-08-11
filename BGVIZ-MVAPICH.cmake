set( LOG4CXX_LIBRARIES /gpfs/bbp.cscs.ch/home/biddisco/bgas/apps/log4cxx/lib/liblog4cxx.so;/gpfs/bbp.cscs.ch/home/biddisco/bgas/apps/apr-1.4.8/lib/libapr-1.so;/gpfs/bbp.cscs.ch/home/biddisco/bgas/apps/apr-util-1.5.3/lib/libaprutil-1.so;/usr/lib64/libxml2.so.2 CACHE STRING "" FORCE)


#------------------------------------------------------------------------------
# Setup MPI variables for BGAS compilation using XL
# Set BGAS 1 before including this file 
#------------------------------------------------------------------------------
#if(BGAS)

  set(SLURM_LIBRARY_PATH /bgsys/bgas/opt/slurm/2.5.7/lib)

set(CMAKE_CXX_COMPILER_FLAGS -mno-qpx)
  
  #
  # Manually setup mpich variables
  #  
  set( MPICH_ROOT /usr/lib64/mvapich2)
  set( MPICH_INCLUDE_ROOT /usr/include/mvapich2-ppc64)
  set( MPICH_LIB_PATH /usr/lib64/mvapich2/lib)
  set( MPI_FOUND 1)
  set( MPI_INCLUDE_PATH ${MPICH_INCLUDE_ROOT} CACHE STRING "" FORCE)
  set( MPI_C_INCLUDE_PATH ${MPICH_INCLUDE_ROOT} CACHE STRING "" FORCE)
  set( MPI_CXX_INCLUDE_PATH ${MPICH_INCLUDE_ROOT} CACHE STRING "" FORCE)

  # * * * * * * * * * * * *  *
  # Warning : on BGAS, order of libs is very important, we must link 
  # slurm;pmi;mpich ....
  # * * * * * * * * * * * *  *

  set(MPI_EXTRA_LIBRARY ${MPICH_LIB_PATH}/libmpich.so;/usr/lib64/libibverbs.so;${MPICH_LIB_PATH}/libopa.so;${MPICH_LIB_PATH}/libmpl.so;rt;pthread CACHE STRING "" FORCE)
#  set(MPI_EXTRA_LIBRARY ${MPICH_LIB_PATH}/libmpich.a;/usr/lib64/libibverbs.so;${MPICH_LIB_PATH}/libopa.a;${MPICH_LIB_PATH}/libmpl.a;rt;pthread CACHE STRING "" FORCE)
  #
  # Newer cmake FindMPI uses per language settings
  #
  set(MPI_C_LIBRARIES       ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)
  set(MPI_CXX_LIBRARIES     ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE)
  set(MPI_LIBRARY           ${MPI_EXTRA_LIBRARY} CACHE STRING "" FORCE )

  set(ppcflr /bgsys/drivers/V1R2M1/ppc64)

  #################################################################
  # Declare link directories for MPI/PAMI/SPI
  #################################################################
  set(MPI_LINK_DIRS
    /bgsys/bgas/opt/slurm/2.5.7/lib
    ${MPICH_LIB_PATH}
  )

  set(MPI_C_COMPILER   /gpfs/bbp.cscs.ch/home/biddisco/bgas/apps/clang/bin/bgclang     CACHE STRING "" FORCE)
  set(MPI_CXX_COMPILER /gpfs/bbp.cscs.ch/home/biddisco/bgas/apps/clang/bin/bgclang++11 CACHE STRING "" FORCE)

   # don't need to call this, but do it anyway just to be sure everything is initialized
#  find_package(MPI REQUIRED)
#endif()
