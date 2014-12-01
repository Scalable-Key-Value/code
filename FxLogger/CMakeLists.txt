
set(FXLOGGER_SOURCES FxLogger.cpp Trace.cpp dlmalloc.c)
set(FXLOGGER_PUBLIC_HEADERS FxLogger.hpp Time.hpp Histogram.hpp Trace.hpp)
set(PROJECT_INCLUDE_NAME FxLogger)
common_library(fxlogger)
