cmake_minimum_required(VERSION 3.6)
project(ConcurrentLib)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# The version number.
set (Tutorial_VERSION_MAJOR 1)
set (Tutorial_VERSION_MINOR 0)

# configure a header file to pass some of the CMake settings
# to the source code
configure_file (
        "${PROJECT_SOURCE_DIR}/TutorialConfig.h.in"
        "${PROJECT_BINARY_DIR}/TutorialConfig.h"
)

# set binary output path
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)

# include the auto-generated header files in build dir
include_directories("${PROJECT_SOURCE_DIR}/build")
include_directories("${PROJECT_SOURCE_DIR}/MathFunctions")
include_directories("${PROJECT_SOURCE_DIR}/CuckoohashingTable")

set(SOURCE_FILES main.cpp)
add_executable(ConcurrentLib ${SOURCE_FILES})
target_link_libraries(ConcurrentLib MathFunctions)
target_link_libraries(ConcurrentLib CuckoohashingTable)

# add math functions
add_subdirectory(MathFunctions)
add_subdirectory(CuckoohashingTable)