#include "headers/FileProcessorBase.hpp"
#include "headers/MapperBase.hpp"
#include "headers/ShufflerBase.hpp"
#include "headers/ReducerBase.hpp"
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <tuple>
#include <vector>
#include <filesystem>
#include <dlfcn.h>

// Overarching function that will orchestrate the main flow
void runOrchestration(const std::string &input_directory);

// Overarching function that will perform Map Reduce operations
void mapReduceWorkflow(const std::string &input_directory);

// file Directory checks
std::vector<std::string> fileDirectoryChecks(const std::string &directory1, const std::string &directory2);

// library handle function
// This function will take a library file and return its corresponding handle as a null pointer
// https://linux.die.net/man/3/dlopen
void* createLibHandle(const char* libraryFile);

// library operations - we will use a template here
// The goal of this template function is to return a function pointer corresponding
// to a factory function assigned to a particular library. It will take location of
// a library file and return the requested typedef for factory function
// We will then use the factory function to create an instance!
template<typename T>
T* createLibFunc(void* libHandle,const char* libraryFile,const char* factoryFunctionHandle){
    // check the type
    static_assert(
            std::is_same<T,create_t>::value ||
            std::is_same<T, destroy_t>::value ||
            std::is_same<T, createMapper_t>::value ||
            std::is_same<T, destroyMapper_t>::value ||
            std::is_same<T, readMapperOp_t>::value ||
            std::is_same<T, destroyMapperOp_t>::value ||
            std::is_same<T, createShuffler_t>::value ||
            std::is_same<T, destroyShuffler_t>::value ||
            std::is_same<T, readShufflerOp_t>::value ||
            std::is_same<T, destroyShufflerOp_t>::value ||
            std::is_same<T, createReducer_t>::value ||
            std::is_same<T, destroyReducer_t>::value ||
            std::is_same<T, readReducerOp_t>::value ||
            std::is_same<T, destroyReducerOp_t>::value,
            "Unsupported Implementation!"
    );
    // raise error if library wasn't loaded
    if(!libHandle){
        throw std::runtime_error("Cannot load library: " + std::string(dlerror()) + "\n");
    }
    // reset errors
    dlerror();
    // load the requested symbol associated with the function pointer which will
    // be used to create instance objects
    // https://linux.die.net/man/3/dlsym
    T* libFunc = (T*)dlsym(libHandle, factoryFunctionHandle);
    // capture any error
    const char* dlsym_error = dlerror();
    // check any error
    if(dlsym_error){
        throw std::runtime_error("Cannot load symbol from- " + std::string(libraryFile) + ":" + dlsym_error + "\n");
    }
    // else return
    return libFunc;
}


int main(int argc, char* argv[]) {
    // Check arguments supplied
    if(argc==1){
        // No arguments were provided!
        std::cout << "No arguments were provided! Please resubmit with input paths!" << std::endl;
    } else{
        // Declare the input file directory being processed
        char* inputDirBase = argv[1];
        // Convert it to string
        std::string inputDir(inputDirBase);
        // Orchestrate the entire operation
        try{
            runOrchestration(inputDir);
        }
        catch(std::runtime_error &runtime_error){
            std::cout << "Exception occurred: " << runtime_error.what() << std::endl;
        }
    }
    // standard return
    return 0;
}

// library handle function
// This function will take a library file and return its corresponding handle as a null pointer
// https://linux.die.net/man/3/dlopen
void* createLibHandle(const char* libraryFile){
    // load library file
    void* libFileHandle = dlopen(libraryFile,RTLD_LAZY);
    // returning this so that it can be used createLibFunction template - used to create class instances
    // and can be used to close independently
    return libFileHandle;
}


// Overarching function that will orchestrate the main flow
void runOrchestration(const std::string &input_directory){
    // Let's check if the input directory exists in the file system
    std::filesystem::path directoryPath(input_directory);
    if(std::filesystem::is_directory(directoryPath)){
        // Let's declare a vector to hold filenames within input directory
        std::vector<std::string> directoryInputFiles;
        // let's find the files
        for(const auto &entry: std::filesystem::directory_iterator(input_directory)){
            directoryInputFiles.push_back(entry.path());
        }
        if(!directoryInputFiles.empty()){
            // Call mapReduceOperations function here!
            std::cout << "Kicking off MapReduce operations..." << std::endl;
            mapReduceWorkflow(input_directory);
        } else {
            std::cout << "No files found to process along " << input_directory << std::endl;
        }
    } else {
        throw std::runtime_error("Directory not found!: " + input_directory );
    }
}

// file Directory checks
std::vector<std::string> fileDirectoryChecks(const std::string &directory1, const std::string &directory2){
    // Map containing input files in directory1
    std::map<std::string, int> dir1MapFiles;
    // Vector containing files that don't exist
    std::vector<std::string> filesDontExist;
    // let's iterate over the directory1
    for(const auto &entry: std::filesystem::directory_iterator(directory1)){
        if(std::filesystem::is_regular_file(entry)){
            std::string fileDirectory = entry.path().string().substr(0, entry.path().string().rfind('/') + 1);
            std::string baseFileName = entry.path().string().substr(entry.path().string().rfind('/') + 1);
            dir1MapFiles.insert({baseFileName,1});
        }
    }
    // check if directory2 is present ...
    // Let's check if that output directory is there!
    if(!std::filesystem::is_directory(directory2)){
        throw std::runtime_error("Output Directory not found!: " + directory2 );
    } else {
        // continue
        for(const auto &entry: std::filesystem::directory_iterator(directory2)){
            if(std::filesystem::is_regular_file(entry)){
                std::string fileDirectory = entry.path().string().substr(0, entry.path().string().rfind('/') + 1);
                std::string baseFileName = entry.path().string().substr(entry.path().string().rfind('/') + 1);
                // Let's create an iterator that will check tempShuffle for the token
                auto mapItr = dir1MapFiles.find(baseFileName);
                // Check if iterator was exhausted
                if (mapItr == dir1MapFiles.end()) {
                    // if parsedToken was not found! - lets create a entry in filesDontExist vector
                    filesDontExist.push_back(baseFileName);
                }
            }
        }
    }
    // return filesDontExist vector
    return filesDontExist;
}

// Overarching function that will perform Map Reduce operations
void mapReduceWorkflow(const std::string &input_directory) {
    try{
        // Load a handle corresponding to FileProcessorInput library
        void* fpInputLibHandle = createLibHandle("./libs/fp/FileProcessorInput.so");
        // Load the FileProcessorInput library!
        // load the symbols associated with function pointer that will create new base instance object of FileProcessorInput
        // it will call the constructor specifically for input operations!
        create_t* create_InputDirectoryFP_Obj = createLibFunc<create_t>(
                fpInputLibHandle,
                "./libs/fp/FileProcessorInput.so",
                "createInputObj");
        // Creating an instance of FileProcessor that handles only input directory paths!
        FileProcessorBase* fpInputDirObj = create_InputDirectoryFP_Obj(
                "input",
                input_directory);
        // run the input operation to load private data members with input Directory data
        fpInputDirObj->runOperation();

        std::cout << "Checking fileinputs.." << std::endl;
        // retrieve private data member that contains all input directory data - simple display here!
        for(const auto& row: fpInputDirObj->getInputDirectoryData()){
            std::cout << row.first << std::endl;
        }

        // Load a handle corresponding to Mapper library
        void* mapLibHandle = createLibHandle("./libs/map/MapperImpl.so");
        // Load the Mapper library!
        // load the symbols associated with function pointer that will create new instance object of MapperBase
        createMapper_t* create_Mapper_Obj = createLibFunc<createMapper_t>(
                mapLibHandle,
                "./libs/map/MapperImpl.so",
                "createInputObj");
        // Create an instance of Mapper
        MapperBase* mapperObj = create_Mapper_Obj(fpInputDirObj->getInputDirectoryData());

        std::cout << "seeing if input made it .." << std::endl;
        for(const auto &ref: mapperObj->getProcessedDirectory()){
            std::cout << ref.first << std::endl;
        }

        // run map operations - this will load the private data member (mapperOutput) with mapped output
        mapperObj->runMapOperation();

        std::cout << "checking mapper ..." << std::endl;
        std::map<std::string, std::vector<std::vector<std::vector<std::tuple<std::string, int, int>>>>> debug = mapperObj->getMapperOutput();
        for(const auto& item:debug){
            std::cout << "WTF" << std::endl;
            std::cout << item.first << std::endl;
        }

        // Load a handle corresponding to FileProcessorMapOutput library
        void* fpMapOpLibHandle = createLibHandle("./libs/fp/FileProcessorMapOutput.so");
        // Load the FileProcessorMapOutput library!
        readMapperOp_t* create_FPMapperOp_Obj = createLibFunc<readMapperOp_t>(
                fpMapOpLibHandle,
                "./libs/fp/FileProcessorMapOutput.so",
                "createInputObj");
        // instantiate a FileProcessorMapOutput object and pass the results of the mapper object earlier
        FileProcessorBase* fpMapOpObj = create_FPMapperOp_Obj("mapper",mapperObj->getMapperOutput());
        // write the results of the mapper to disk!
        fpMapOpObj->runOperation();
        // mapper output directory
        std::cout << "Mapper results have been written to: " << fpMapOpObj->getMapperOutputDirectory() << std::endl;

        // Load a handle corresponding to Shuffler library
        void* shufLibHandle = createLibHandle("./libs/shuffle/ShufflerImpl.so");
        // Load the Shuffler library!
        createShuffler_t* create_Shuffler_Obj = createLibFunc<createShuffler_t>(
                shufLibHandle,
                "./libs/shuffle/ShufflerImpl.so",
                "createInputObj");
        // instantiate a ShufflerImpl object and pass the mapper output directory from earlier
        ShufflerBase* shufflerObj = create_Shuffler_Obj(fpMapOpObj->getMapperOutputDirectory());
        // perform shuffle operations!
        shufflerObj->runShuffleOperation();

        // Load a handle corresponding to FileProcessorShufOutput library
        void* fpShufOpLibHandle = createLibHandle("./libs/fp/FileProcessorShufOutput.so");
        // Load the FileProcessorShufOutput library
        readShufflerOp_t* create_FPShufflerOp_Obj = createLibFunc<readShufflerOp_t>(
                fpShufOpLibHandle,
                "./libs/fp/FileProcessorShufOutput.so",
                "createInputObj");
        // instantiate a FileProcessorShufOutput object and pass the raw output of the shuffler from earlier
        FileProcessorBase* fpShufOpObj = create_FPShufflerOp_Obj("shuffler",shufflerObj->getShuffledOutput());
        // Write the results ..
        fpShufOpObj->runOperation();
        // display shuffled directory...
        std::cout << "Shuffled results have been written to: " << fpShufOpObj->getShufflerOutputDirectory() << std::endl;

        // Load a handle corresponding to Reducer library
        void* redLibHandle = createLibHandle("./libs/reduce/ReducerImpl.so");
        // load the Reducer library
        createReducer_t* create_Reducer_Obj = createLibFunc<createReducer_t>(
                redLibHandle,
                "./libs/reduce/ReducerImpl.so",
                "createInputObj");
        // instantiate a ReducerImpl object and pass the mapper output directory from earlier
        ReducerBase* reducerObj = create_Reducer_Obj(fpShufOpObj->getShufflerOutputDirectory());
        // run reduce operations - which will save the reduced result as a private data member
        reducerObj->runReduceOperations();

        // Load a handle corresponding to FileProcessorRedOutput library
        void* fpRedOpLibHandle = createLibHandle("./libs/fp/FileProcessorRedOutput.so");
        // load the FileProcessorRedOutput library
        readReducerOp_t* create_FPReducerOp_Obj = createLibFunc<readReducerOp_t>(
                fpRedOpLibHandle,
                "./libs/fp/FileProcessorRedOutput.so",
                "createInputObj");
        // instantiate a FileProcessorRedOutput object and pass the raw output of the reducer from earlier
        FileProcessorBase* fpRedOpObj = create_FPReducerOp_Obj("reducer",reducerObj->getReducedOutput());
        // Write the results ..
        fpRedOpObj->runOperation();
        std::string outputDirectory = fpRedOpObj->getFinalOutputDirectory();
        std::cout << "Final results have been written to: " << outputDirectory << std::endl;

        // Let's create necessary factory functions to remove the FileProcessorInput instance
        destroy_t* destroy_InputDirectoryFP_Obj = createLibFunc<destroy_t>(
                fpInputLibHandle,
                "./libs/fp/FileProcessorInput.so",
                "removeInputObj"
        );
        // Let's create necessary factory functions to remove the Mapper instance
        destroyMapper_t* destroy_Mapper_Obj = createLibFunc<destroyMapper_t>(
                mapLibHandle,
                "./libs/map/MapperImpl.so",
                "removeInputObj"
        );
        // Let's create necessary factory functions to remove the FileProcessorMapOutput instance
        destroyMapperOp_t* destroy_FPMapperOp_Obj = createLibFunc<destroyMapperOp_t>(
                fpMapOpLibHandle,
                "./libs/fp/FileProcessorMapOutput.so",
                "removeInputObj"
        );
        // Let's create necessary factory functions to remove the Shuffler instance
        destroyShuffler_t* destroy_Shuffler_Obj = createLibFunc<destroyShuffler_t>(
                shufLibHandle,
                "./libs/shuffle/ShufflerImpl.so",
                "removeInputObj"
        );
        // Let's create necessary factory functions to remove the FileProcessorShufOutput instance
        destroyShufflerOp_t* destroy_FPShufflerOp_Obj = createLibFunc<destroyShufflerOp_t>(
                fpShufOpLibHandle,
                "./libs/fp/FileProcessorShufOutput.so",
                "removeInputObj"
        );
        // Let's create necessary factory functions to remove the Reducer instance
        destroyReducer_t* destroy_Reducer_Obj = createLibFunc<destroyReducer_t>(
                redLibHandle,
                "./libs/reduce/ReducerImpl.so",
                "removeInputObj"
        );
        // Let's create necessary factory functions to remove the FileProcessorRedOutput instance
        destroyReducerOp_t* destroy_FPReducerOp_Obj = createLibFunc<destroyReducerOp_t>(
                fpRedOpLibHandle,
                "./libs/fp/FileProcessorRedOutput.so",
                "removeInputObj"
        );

        // Remove the instances!
        destroy_InputDirectoryFP_Obj(fpInputDirObj);
        destroy_Mapper_Obj(mapperObj);
        destroy_FPMapperOp_Obj(fpMapOpObj);
        destroy_Shuffler_Obj(shufflerObj);
        destroy_FPShufflerOp_Obj(fpShufOpObj);
        destroy_Reducer_Obj(reducerObj);
        destroy_FPReducerOp_Obj(fpRedOpObj);

        // unload the handles
        // FileProcessorInput
        dlclose(fpInputLibHandle);
        //Mapper
        dlclose(mapLibHandle);
        // FileProcessorMapOutput
        dlclose(fpMapOpLibHandle);
        // Shuffler
        dlclose(shufLibHandle);
        // FileProcessorShufflerOutput
        dlclose(fpShufOpLibHandle);
        // Reducer
        dlclose(redLibHandle);
        // FileProcessorReducerOutput
        dlclose(fpRedOpLibHandle);

        // Vector of files...
        std::vector<std::string> fileDontExist1to2 = fileDirectoryChecks(input_directory, outputDirectory);
        std::vector<std::string> fileDontExist2to1 = fileDirectoryChecks(outputDirectory, input_directory);

        // Create a SUCCESS indicator if filesDontExist vector is empty
        if(fileDontExist1to2.empty() && fileDontExist2to1.empty()){
            std::ofstream successFile;
            successFile.open(outputDirectory + "/" + "SUCCESS.ind");
            successFile.close();
        } else {
            throw std::runtime_error("There are missing files!");
        }

    }
    catch(std::runtime_error &runtime_error){
        // Exception occurred loading the FileProcessor library!
        std::cout << "Exception occurred: " << runtime_error.what() << std::endl;
    }
}