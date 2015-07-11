// Copyright (C) 2015 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package folder

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/syncthing/syncthing/internal/osutil"
)

type Folder struct {
	ID   string
	Path string
}

const (
	MarkerName = ".stfolder"
)

func New(id, path string) *Folder {
	return &Folder{
		ID:   id,
		Path: expandPath(path),
	}
}

func (f *Folder) CreateMarker() error {
	markerPath := filepath.Join(f.Path, MarkerName)
	fd, err := os.Create(markerPath)
	if err != nil {
		return err
	}
	fd.Close()
	osutil.HideFile(markerPath)

	return nil
}

func (f *Folder) CheckMarker() error {
	_, err := os.Stat(filepath.Join(f.Path, MarkerName))
	return err
}

func expandPath(path string) string {
	// Attempt tilde expansion; leave unchanged in case of error
	path, _ = osutil.ExpandTilde(path)

	// Attempt absolutification; leave unchanged in case of error
	if !filepath.IsAbs(path) {
		// Abs() looks like a fairly expensive syscall on Windows, while
		// IsAbs() is a whole bunch of string mangling. I think IsAbs() may be
		// somewhat faster in the general case, hence the outer if...
		path, _ = filepath.Abs(path)
	}

	// Attempt to enable long filename support on Windows. We may still not
	// have an absolute path here if the previous steps failed.
	if runtime.GOOS == "windows" && filepath.IsAbs(path) && !strings.HasPrefix(path, `\\`) {
		path = `\\?\` + path
	}

	return path
}
