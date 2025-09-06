cmake_minimum_required(VERSION 3.5.0)
project(Orchestrator VERSION 0.1.0 LANGUAGES C CXX)

file(GLOB SOURCES "src/*.cpp" "include/*.h")

add_executable(Orchestrator ${SOURCES})

include(CTest)
enable_testing()

