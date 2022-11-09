//
// Created by sakkammadam on 11/6/22.
//

#ifndef MRHOPE_MAPPERBASE_H
#define MRHOPE_MAPPERBASE_H

#include <string>
#include <tuple>
#include <vector>
#include <map>
#include <sstream>

class MapperBase{
// Private data members
private:
    std::map<std::string, std::vector<std::vector<std::string>>> processedDirectory;
    std::map<std::string, std::vector<std::vector<std::vector<std::tuple<std::string, int, int>>>>> mapperOutput;

public:
    // Default Constructor
    MapperBase(){
      // Nothing here
    };

    // Initialization Constructor
    explicit MapperBase(
            // Get the processed directory object
            const std::map<std::string, std::vector<std::vector<std::string>>> &processed_directory
    ){
        this->setProcessedDirectory(processed_directory);
    }

    // Setters
    // This will set the processedDirectory private data member within the Mapper object
    void setProcessedDirectory(const std::map<std::string, std::vector<std::vector<std::string>>> &processed_directory){
        this->processedDirectory = processed_directory;
    }

    // Setters
    // This will store the processed map output as a private data member
    void setMapperOutputData(const std::map<std::string, std::vector<std::vector<std::vector<std::tuple<std::string, int, int>>>>> &mapped_output){
        this->mapperOutput = mapped_output;
    }

    // Getters
    // This will retrieve the processedDirectory private data member within the Mapper object
    std::map<std::string, std::vector<std::vector<std::string>>> getProcessedDirectory(){
        return this->processedDirectory;
    }

    // This will retrieve the mapped output data private data member within the Mapper object
    std::map<std::string, std::vector<std::vector<std::vector<std::tuple<std::string, int, int>>>>> getMapperOutput(){
        return this->mapperOutput;
    }

    // Virtual method to run operations
    // Primary method that will act on processed input data and create a map
    virtual void runMapOperation() = 0;

};

// Class factories -
typedef MapperBase* createMapper_t(const std::map<std::string, std::vector<std::vector<std::string>>> &inputData);
typedef void destroyMapper_t(MapperBase*);


#endif //MRHOPE_MAPPERBASE_H
