cmake_minimum_required(VERSION 3.16)
project(azure_credentials LANGUAGES C CXX)

if(WIN32)
  message(FATAL_ERROR "azure_credentials uses POSIX pthreads; build under Linux or WSL instead.")
endif()

get_filename_component(KERNEL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(KERNEL_TARGET_DIR "${KERNEL_ROOT}/target" CACHE PATH "Kernel Cargo target directory")
set(KERNEL_HEADER_DIR "${KERNEL_ROOT}/target/ffi-headers" CACHE PATH "Generated C header directory")
set(KERNEL_PROFILE "debug" CACHE STRING "Kernel library profile directory")

set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
add_executable(azure_credentials azure_credentials.c)
set_target_properties(azure_credentials PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
  C_EXTENSIONS NO
  BUILD_RPATH "${KERNEL_TARGET_DIR}/${KERNEL_PROFILE}")
target_compile_definitions(azure_credentials PRIVATE DEFINE_DEFAULT_ENGINE_BASE)
target_include_directories(azure_credentials PRIVATE "${KERNEL_HEADER_DIR}")
target_link_directories(azure_credentials PRIVATE "${KERNEL_TARGET_DIR}/${KERNEL_PROFILE}")
target_link_libraries(azure_credentials PRIVATE delta_kernel_ffi Threads::Threads)
target_compile_options(azure_credentials PRIVATE -Wall -Wextra -Werror)

add_custom_target(azure_credentials_cpp_abi ALL
  COMMAND "${CMAKE_CXX_COMPILER}" -std=c++17 -x c++ -pthread -fsyntax-only
    -DDEFINE_DEFAULT_ENGINE_BASE -Wall -Wextra -Werror
    -I "${KERNEL_HEADER_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/azure_credentials.c"
  VERBATIM)

include(CTest)
if(BUILD_TESTING)
  add_test(NAME azure_credentials_ownership COMMAND azure_credentials --self-test)
  set_tests_properties(azure_credentials_ownership PROPERTIES TIMEOUT 30)
endif()