# list all required source files for the router here
set(SKV_CNKROUTER_SOURCES
 it_api_cnk_multiplex_router.cpp
)

# list all required libraries for the router here:
set(SKV_CNKROUTER_LINK_LIBRARIES pthread m dl ${OFED_LIBRARIES}
  ${MPI_CXX_LIBRARIES} skv_common it_api fxlogger)


common_application(SKV_CNKRouter)

# if needed, uncomment and add additional macro definitions here
#  (excluding log or trace)

set_target_properties(SKV_CNKRouter PROPERTIES
  COMPILE_DEFINITIONS "${SKV_IT_API_LOGGING}" )

