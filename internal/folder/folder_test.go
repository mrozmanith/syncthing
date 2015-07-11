// Copyright (C) 2015 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package folder

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateCheckMarker(t *testing.T) {
	os.RemoveAll("testdata")
	folder := New("default", "testdata")

	// A nonexistent folder does not have a marker

	if folder.CheckMarker() == nil {
		t.Fatal("Unexpected nil error from nonexistent dir")
	}

	// A marker cannot be created in a nonexistent dir

	if err := folder.CreateMarker(); err == nil {
		t.Fatal("Unexpected nil error when creating marker in nonexistent dir")
	}

	os.Mkdir("testdata", 0777)

	// An empty dir doesn't have a marker

	if folder.CheckMarker() == nil {
		t.Fatal("Unexpected nil error from blank dir")
	}

	// A marker can be crated there though

	if err := folder.CreateMarker(); err != nil {
		t.Fatal("Unexpected error creating marker:", err)
	}

	// And afterwards it should exist

	if err := folder.CheckMarker(); err != nil {
		t.Fatal("Unexpected error checking marker:", err)
	}

	os.Remove(filepath.Join("testdata", MarkerName))

	// But not after we've removed it

	if folder.CheckMarker() == nil {
		t.Fatal("Unexpected nil error after removing marker")
	}
}
