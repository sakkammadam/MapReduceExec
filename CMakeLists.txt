cmake_minimum_required(VERSION 3.23)
project(MapReduceExec)

set(CMAKE_CXX_STANDARD 17)

add_executable(MapReduceExec main.cpp
        headers/FileProcessorBase.hpp
        headers/MapperBase.hpp
        headers/ShufflerBase.hpp
        headers/ReducerBase.hpp
        )

target_link_libraries(MapReduceExec ${CMAKE_DL_LIBS})