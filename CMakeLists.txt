cmake_minimum_required(VERSION 3.14)
project(frankamera)

set(CMAKE_CXX_STANDARD 14)

add_executable(frankamera main.cpp Hikvision.cpp)

# Hikvision SDK files
include_directories(vendor/HCNetSDKV6.0.2.35_build20190411_Linux64_EN/incEn)
find_library(HIKVISION_HCNETSDK hcnetsdk vendor/HCNetSDKV6.0.2.35_build20190411_Linux64_EN/lib)
target_link_libraries(frankamera ${HIKVISION_HCNETSDK})