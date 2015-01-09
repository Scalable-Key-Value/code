# Find the SPI libraries for BGQ

# - Try to find the BG/Q SPI library
# Once done this will define
#
#  SPI_ROOT - Set this variable to the root installation
#
# Read-Only variables:
#  SPI_FOUND - system has the SPI library
#  SPI_INCLUDE_DIR - the SPI include directory
#  SPI_LIBRARIES - The libraries needed to use SPI
#  SPI_VERSION - This is set to $major.$minor.$patch (eg. 0.9.8)


include(FindPackageHandleStandardArgs)

if(SPI_FIND_REQUIRED)
  set(_SPI_output_type FATAL_ERROR)
  set(_SPI_output 1)
else()
  set(_SPI_output_type STATUS)
  if(SPI_FIND_QUIETLY)
    set(_SPI_output)
  else()
    set(_SPI_output 1)
  endif()
endif()

find_path(_SPI_INCLUDE_DIR kernel/rdma.h
  HINTS $ENV{SPI_ROOT} ${SPI_ROOT}
  PATH_SUFFIXES include
  PATHS /bgsys/drivers/ppcfloor)

# find the libs
find_library(SPI_CNK_LIBRARY SPI_cnk
  HINTS $ENV{SPI_ROOT} ${SPI_ROOT}
  PATH_SUFFIXES lib lib64
  PATHS /bgsys/drivers/ppcfloor)

find_library(SPI_L1P_LIBRARY SPI_l1p
  HINTS $ENV{SPI_ROOT} ${SPI_ROOT}
  PATH_SUFFIXES lib lib64
  PATHS /bgsys/drivers/ppcfloor)

if(NOT SPI_CNK_LIBRARY OR NOT SPI_L1P_LIBRARY  OR  NOT _SPI_INCLUDE_DIR)
  # Zero out everything, we didn't meet version requirements
  set(SPI_FOUND FALSE)
  set(SPI_LIBRARY)
  set(_SPI_INCLUDE_DIR)
  set(SPI_INCLUDE_DIRS)
  set(SPI_LIBRARIES)
  if(_SPI_output)
    message(FATAL_ERROR
      "Couldn't find SPI library. cnk:${SPI_CNK_LIBRARY} l1p:${SPI_L1P_LIBRARY}")
  endif()
 
else()
  set(SPI_INCLUDE_DIRS ${_SPI_INCLUDE_DIR})
  set(SPI_LIBRARIES ${SPI_CNK_LIBRARY} ${SPI_L1P_LIBRARY})
  if(_SPI_output)
    message(STATUS
      "Found SPI library in ${SPI_INCLUDE_DIRS};${SPI_LIBRARIES}")
  endif()
endif()
