#!/usr/bin/env python
#
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This program creates a manifest.json file from a directory of parcels and
# places the file in the same directory as the parcels.
# Once created, the directory can be served over http as a parcel repository.
#
# This program is slighly modified to allow for updating manifest files
# and adds a file lock to avoid race conditions on updating the files.
#
# Usage: make_manifest.py create /path/to/parcels
#        make_manifest.py update /path/to/parcels CDH-5.4.0.parcel CDH-5.4.1.parcel

import hashlib
import json
import os
import re
import sys
import tarfile
import time
import errno


class FileLockException(Exception):
    pass


class FileLock(object):
  """ A file locking mechanism that has context-manager support so
  you can use it in a with statement.

  Implementation based on:

  http://www.evanfosmark.com/2009/01/cross-platform-file-locking-support-in-python/
  """

  def __init__(self, lockfile, timeout=10, delay=0.05):
    self.is_locked = False
    self.lockfile = lockfile
    self.fd = None
    self.timeout = timeout
    self.delay = delay

  def acquire(self):
    """ Acquire the lock, if possible.

    This will not retry but sets `is_locked` corrispondingly.
    """
    start_time = time.time()
    while True:
      try:
        self.fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        break
      except OSError as e:
        if e.errno != errno.EEXIST:
          raise
        # FIXME we should check if lockfile was created more than 1h ago, if so we should
        # consider it stale
        if (time.time() - start_time) >= self.timeout:
          raise FileLockException("Timeout occured.")
        time.sleep(self.delay)
    self.is_locked = True


  def release(self):
    """ Get rid of the lock by deleting the lockfile.
    When working in a `with` statement, this gets automatically
    called at the end.
    """
    if self.is_locked:
      os.close(self.fd)
      os.unlink(self.lockfile)
      self.is_locked = False

  def __enter__(self):
    if not self.is_locked:
      self.acquire()
      return self

  def __exit__(self, type, value, traceback):
    if self.is_locked:
      self.release()

  def __del__(self):
    """ Make sure that the FileLock instance doesn't leave a lockfile
    lying around.
    """
    self.release()


def _get_parcel_dirname(parcel_name):
  """
  Extract the required parcel directory name for a given parcel.

  eg: CDH-5.0.0-el6.parcel -> CDH-5.0.0
  """
  parts = re.match(r"^(.*?)-(.*)-(.*?)$", parcel_name).groups()
  return parts[0] + '-' + parts[1]

def _safe_copy(key, src, dest):
  """
  Conditionally copy a key/value pair from one dictionary to another.

  Nothing is done if the key is not present in the source dictionary
  """
  if key in src:
    dest[key] = src[key]

def make_manifest(path, timestamp=time.time(), files=None, manifest=None):
  """
  Make a manifest.json document from the contents of a directory.

  This function will scan the specified directory, identify any parcel files
  in it, and then build a manifest from those files. Certain metadata will be
  extracted from the parcel and copied into the manifest.

  @param path: The path of the directory to scan for parcels
  @param timestamp: Unix timestamp to place in manifest.json
  @return: the manifest.json as a string
  """
  if not manifest:
    manifest = {}
    manifest['parcels'] = []

  if not files:
    files = [f for f in os.listdir(path) if f.endswith('.parcel')]

  entries = make_entries(path, files)

  manifest['parcels'] = manifest['parcels'] + entries
  manifest['lastUpdated'] = int(timestamp * 1000)
  return json.dumps(manifest, indent=4, separators=(',', ': '))


def make_entries(path, files):
  entries = []
  for f in files:
    print("Found parcel %s" % (f,))
    entry = {}
    entry['parcelName'] = f

    fullpath = os.path.join(path, f)

    with open(fullpath, 'rb') as fp:
      entry['hash'] = hashlib.sha1(fp.read()).hexdigest()

    with tarfile.open(fullpath, 'r') as tar:
      try:
        json_member = tar.getmember(os.path.join(_get_parcel_dirname(f),
                                    'meta', 'parcel.json'))
      except KeyError:
        print("Parcel does not contain parcel.json")
        continue
      try:
        parcel = json.loads(tar.extractfile(json_member).read().decode(encoding='UTF-8'))
      except:
        print("Failed to parse parcel.json")
        continue
      _safe_copy('depends', parcel, entry)
      _safe_copy('replaces', parcel, entry)
      _safe_copy('conflicts', parcel, entry)
      _safe_copy('components', parcel, entry)

      try:
        notes_member = tar.getmember(os.path.join(_get_parcel_dirname(f),
                                     'meta', 'release-notes.txt'))
        entry['releaseNotes'] = tar.extractfile(notes_member).read().decode(encoding='UTF-8')
      except KeyError:
        # No problem if there's no release notes
        pass
    entries.append(entry)
  return entries


if __name__ == "__main__":
  path = os.path.curdir
  _ = sys.argv.pop(0)
  cmd = sys.argv.pop(0)
  assert cmd in ('create', 'update')
  path = sys.argv.pop(0)

  fnames = sys.argv[:]

  path_to_manifest = os.path.join(path, 'manifest.json')
  path_to_filelock = path_to_manifest + '.lock'
  with FileLock(path_to_filelock, timeout=60*10, delay=5):
    if cmd == 'create':
      manifest = None
    elif cmd == 'update':
      if not os.path.exists(path_to_manifest):
        raise ValueError('manifest.json expected at {} but not found'.format(path_to_manifest))
      with open(path_to_manifest, 'r') as fp:
        manifest = json.load(fp)

    print("Scanning directory: %s" % (path))

    manifest = make_manifest(path, files=fnames, manifest=manifest)

    with open(path_to_manifest, 'w') as fp:
      fp.write(manifest)
