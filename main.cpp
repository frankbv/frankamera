#include <iostream>

#include "Hikvision.h"

int main(int argc, char **argv)
{
    auto hv = Hikvision();

    std::cout << "Hikvision SDK version: " << hv.getVersion() << std::endl;

    return 0;
}