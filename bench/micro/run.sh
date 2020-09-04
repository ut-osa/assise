#! /bin/bash

PATH=$PATH:.
PROJECT_ROOT=../..

LD_PRELOAD=$PROJECT_ROOT/libfs/build/libmlfs.so ${@}
