cmake_minimum_required(VERSION 3.0.0)
project(testsnapshot VERSION 0.1.0 LANGUAGES CXX)

add_executable(testsnapshot main.cpp)

set_property(TARGET testsnapshot PROPERTY CXX_STANDARD 17)
target_link_libraries(testsnapshot cephfs)
set_target_properties(testsnapshot PROPERTIES COMPILE_DEFINITIONS "_FILE_OFFSET_BITS=64")
set_target_properties(testsnapshot PROPERTIES COMPILE_FLAGS "-g -O0")
