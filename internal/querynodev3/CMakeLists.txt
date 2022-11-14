# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required( VERSION 3.18 )

# To debug memory issues, you can comment out these below two lines.
# Also, they are identical to that you define these environments:
# CFLAGS="-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address"
# CXXFLAGS="-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address"
# LDFLAGS="-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address"

# add_compile_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
# add_link_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)

if ( APPLE )
    set( CMAKE_CROSSCOMPILING TRUE )
    set( RUN_HAVE_GNU_POSIX_REGEX 0 )
    set( CMAKE_C_COMPILER "/usr/local/opt/llvm/bin/clang" )
    set( CMAKE_CXX_COMPILER "/usr/local/opt/llvm/bin/clang++" )
endif ()

add_definitions(-DELPP_THREAD_SAFE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
message( STATUS "Building using CMake version: ${CMAKE_VERSION}" )

project(querynode)
include(CheckCXXCompilerFlag)
if ( APPLE )
    message(STATUS "==============Darwin Environment==============")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I/usr/local/opt/llvm/include -I/usr/local/include -I/usr/local/opt/libomp/include -L/usr/local/opt/libomp/lib")
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    message(STATUS "==============Linux Environment===============")
    set(LINUX TRUE)
elseif ( MSYS )
    message( STATUS "==============MSYS Environment===============" )
else ()
    message(FATAL_ERROR "Unsupported platform!" )
endif ()

if (CMAKE_COMPILER_IS_GNUCC)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 11.99)
        # ignore deprecated declarations for gcc>=12
        # TODO: this workaround may removed when protobuf upgraded
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-declarations")
    endif ()
endif ()

set( CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake" )
include( Utils )

# **************************** Build time, type and code version ****************************
get_current_time( BUILD_TIME )
message( STATUS "Build time = ${BUILD_TIME}" )

get_build_type( TARGET BUILD_TYPE DEFAULT "Release" )
message( STATUS "Build type = ${BUILD_TYPE}" )

get_milvus_version( TARGET MILVUS_VERSION DEFAULT "2.0" )
message( STATUS "Build version = ${MILVUS_VERSION}" )

get_last_commit_id( LAST_COMMIT_ID )
message( STATUS "LAST_COMMIT_ID = ${LAST_COMMIT_ID}" )

set( CMAKE_EXPORT_COMPILE_COMMANDS ON )

# **************************** Project ****************************
project( milvus VERSION "${MILVUS_VERSION}" )

set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED on )

set( MILVUS_SOURCE_DIR      ${PROJECT_SOURCE_DIR} )
set( MILVUS_BINARY_DIR      ${PROJECT_BINARY_DIR} )
set( MILVUS_ENGINE_SRC      ${PROJECT_SOURCE_DIR}/src )
set( MILVUS_THIRDPARTY_SRC  ${PROJECT_SOURCE_DIR}/thirdparty )

# This will set RPATH to all excutable TARGET
# self-installed dynamic libraries will be correctly linked by excutable
set( CMAKE_INSTALL_RPATH "/usr/lib" "${CMAKE_INSTALL_PREFIX}/lib" "${CMAKE_INSTALL_PREFIX}/lib64" )
set( CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE )

# **************************** Dependencies ****************************
include( CTest )
include( BuildUtils )
include( DefineOptions )
using_ccache_if_defined(MILVUS_USE_CCACHE)

include( ExternalProject )
include( GNUInstallDirs )
include( FetchContent )
include_directories( thirdparty )
set( FETCHCONTENT_BASE_DIR ${MILVUS_BINARY_DIR}/3rdparty_download )
set( FETCHCONTENT_QUIET OFF )
# include( ThirdPartyPackages )

include_directories( ../core )
include_directories( src )