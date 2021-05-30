#! /usr/bin/env python

from __future__ import print_function

import os
import re
import sys

GB = 1024 ** 3

def main():
  if len(sys.argv) < 4:
    print("Usage: {} <NVM GB> <SSD GB/0> <HDD GB/0>".format(
      sys.argv[0]))
    print("Note: additional args can be supplied to cover more devices.")
    return -1

  utils_dir = os.path.dirname(os.path.realpath(__file__))
  project_root = os.path.realpath(os.path.join(utils_dir, ".."))
  storage_dir = os.path.join(project_root, "libfs", "src", "storage")
  if not os.path.exists(storage_dir):
    print("Error: Bad working directory for this script")
    return -1

  storage_h = os.path.join(storage_dir, "storage.h")

  if not os.path.exists(storage_h):
    print("Error: Cannot find {}".format(storage_h))
    return -1

  f_contents = str()
  with open(storage_h, "r+") as f:
    f_contents = f.read()

    pattern = r'(^static uint64\_t dev\_size[^{}]*) {[^{}]*}(.*)'
    replace = r'\1 {0UL'

    for i in range(1,len(sys.argv)):
        replace += r', {}UL'.format(int(sys.argv[i]) * GB)

    replace += r'}\2'

    f_contents = re.sub(pattern, replace, f_contents, flags=re.M)
    f.seek(0)
    f.truncate(0)
    f.write(f_contents)

  return 0


if __name__ == "__main__":
  exit(main())
