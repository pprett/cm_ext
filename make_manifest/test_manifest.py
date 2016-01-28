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

import json
import make_manifest
import test_deep_equality
import unittest

class TestMakeManifest(unittest.TestCase):
  def test_make_manifest(self):
    manifest_json = make_manifest.make_manifest('test_artifacts', 0)
    with open('test_artifacts/expected.json') as fp:
      expected_json = fp.read()
      manifest = json.loads(manifest_json)
      expected = json.loads(expected_json)
      test_deep_equality.deep_eq(manifest, expected, _assert=True)

  def test_update_manifest(self):
    manifest_json = make_manifest.make_manifest('test_artifacts_update', timestamp=0,
                                                files=['CDH-5.0.0-0.cdh5b2.p0.30-precise.parcel'])
    with open('test_artifacts/expected.json') as fp:
      expected_json = fp.read()
      manifest = json.loads(manifest_json)
      expected = json.loads(expected_json)
      test_deep_equality.deep_eq(manifest, expected, _assert=True)

    old_manifest = json.loads(manifest_json)

    manifest_json = make_manifest.make_manifest('test_artifacts_update', timestamp=1,
                                                files=['CDH-5.0.0-0.cdh5b2.p0.31-precise.parcel',
                                                       'CDH-5.0.0-0.cdh5b2.p0.32-precise.parcel'],
                                                manifest=json.loads(manifest_json))

    manifest = json.loads(manifest_json)
    test_deep_equality.deep_eq(manifest['parcels'][:1], old_manifest['parcels'], _assert=True)
    assert len(manifest['parcels']) == 3


if __name__ == "__main__":
  unittest.main()
