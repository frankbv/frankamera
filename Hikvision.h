#ifndef FRANKAMERA_HIKVISION_H
#define FRANKAMERA_HIKVISION_H

#include <string>

#include <HCNetSDK.h>

class Hikvision {
public:
    Hikvision();
    ~Hikvision();

    std::string getVersion();
};

#endif //FRANKAMERA_HIKVISION_H
