#include <sstream>

#include "Hikvision.h"

Hikvision::Hikvision()
{
    NET_DVR_Init();
}

Hikvision::~Hikvision()
{
    NET_DVR_Cleanup();
}

std::string Hikvision::getVersion()
{
    std::ostringstream version;
    unsigned int sdkBuildVersion = NET_DVR_GetSDKBuildVersion();

    version << ((sdkBuildVersion & 0xFF000000) >> 24) << '.'
            << ((sdkBuildVersion & 0x00FF0000) >> 16) << '.'
            << ((sdkBuildVersion & 0x0000FF00) >> 8) << '.'
            << (sdkBuildVersion & 0x000000FF);

    return version.str();
}
