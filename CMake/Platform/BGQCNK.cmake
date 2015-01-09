add_definitions(-DPK_CNK)
set(COMMON_LIBRARY_TYPE STATIC)

set(SKV_COMM_API_TYPE "sockets_routed" CACHE STRING
 "Select a database backend {verbs, sockets, sockets_routed}")

set(SPI_ROOT /bgsys/drivers/ppcfloor/spi)
