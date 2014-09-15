# Copyright (c) 2012 Stefan Eilemann <eile@eyescale.ch>

# Info: http://www.itk.org/Wiki/CMake:Component_Install_With_CPack

set(CPACK_PACKAGE_VENDOR "www.ibm.com")
set(CPACK_PACKAGE_CONTACT "Stefan.Eilemann@epfl.ch")
set(CPACK_PACKAGE_DESCRIPTION_FILE ${PROJECT_SOURCE_DIR}/README)
set(CPACK_RESOURCE_FILE_README ${PROJECT_SOURCE_DIR}/README)

set(CPACK_DEBIAN_PACKAGE_DEPENDS "libstdc++6")

set(CPACK_MACPORTS_CATEGORY devel)

include(CommonCPack)
include(OSSCPack)
