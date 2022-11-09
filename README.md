# Introduction

This repository relies on the [MapReduceLib](https://github.com/sakkammadam/MapReduceLib) created as part of 
CSE687's phase 2 implementation.

From the previous repo., we will copy the library files (* .so) and the interface files (*.hpp). The interface files
will reflect the abstract class implementations.

## Project structure

This repo. has been broken into sub-folders.

### headers

This folder will host all the base class implementations

### libs

This folder will host all the library files associated with each class.

#### fp

This folder will host all the library files associated with FileProcessor implementations.
Please refer to previous repo. for further details.

#### map

This folder will host all the library files associated with Mapper implementations.
Please refer to previous repo. for further details.

#### shuffle

This folder will host all the library files associated with Shuffler implementations.
Please refer to previous repo. for further details.

#### reduce

This folder will host all the library files associated with Reducer implementations.
Please refer to previous repo. for further details.

### main.cpp

This is the driver code that will execute MapReduce operations against a directory.
It has the following components - 

1) runOrchestration 
   * This function acts as overall orchestration mechanism
   * It checks for arguments and valid paths
   * It will kick off mapReduceWorkflow function
2) fileDirectoryChecks
    * Helper function that will check if all the files have been processed
    * Creates a SUCCESS.ind if checks pass
3) createLibHandle
    * This function will create a null pointer against the library file
4) createLibFunc
    * This is template function that will be used to create different factory functions associated with required class instances.
    * It uses the dlopen API to create the necessary explicit linkage
5) mapReduceWorkflow
    * This is the entire Map Reduce pipeline
    * It relies on createLibFunc to create factory functions which are internally used to create class instances
    * It follows this workflow - 
      * Load all data in a directory in a memory object
      * Tokenize data as part of Mapper operations
      * Load mapped data to disk
      * Read mapped data from disk to shuffler and perform shuffle operations
      * Write shuffled data to disk
      * Read shuffled data from disk to reduce and perform reduce operations
      * Write reduced data to disk
    * At the end of all operations, we release the instances and close out the file handle,
      * This is in accordance with dlopen API covered [here](https://tldp.org/HOWTO/html_single/C++-dlopen/).


### Building

To build the project, run the following command
    
    g++ -std=c++17 -o MRExec main.cpp headers/FileProcessorBase.hpp headers/MapperBase.hpp headers/ReducerBase.hpp headers/ShufflerBase.hpp -ldl

To execute 

    ./MRExec /path/to/files/ > logs/run_date '+%Y%m%d%H%M%S'.log