package model

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/syncthing/syncthing/internal/osutil"
)

type Folder struct {
	id   string
	path string
}

func New(id, path string) *Folder {
	return &Folder{
		id:   id,
		path: expandPath(path),
	}
}

// CheckFolderHealth checks the folder for common errors and returns the
// current folder error, or nil if the folder is healthy.
func (f folder) CheckFolderHealth(emptyOK bool) error {
	fi, err := os.Stat(f.path)
	if !emptyOK {
		// Safety check. If the cached index contains files but the
		// folder doesn't exist, we have a problem. We would assume
		// that all files have been deleted which might not be the case,
		// so mark it as invalid instead.
		if err != nil || !fi.IsDir() {
			err = errors.New("folder path missing")
		} else if !f.HasMarker() {
			err = errors.New("folder marker missing")
		}
	} else if os.IsNotExist(err) {
		// If we don't have any files in the index, and the directory
		// doesn't exist, try creating it.
		err = osutil.MkdirAll(folder.Path(), 0700)
		if err == nil {
			err = f.CreateMarker()
		}
	} else if !f.HasMarker() {
		// If we don't have any files in the index, and the path does exist
		// but the marker is not there, create it.
		err = f.CreateMarker()
	}

	m.fmut.RLock()
	runner, runnerExists := m.folderRunners[folder.ID]
	m.fmut.RUnlock()

	var oldErr error
	if runnerExists {
		_, _, oldErr = runner.getState()
	}

	if err != nil {
		if oldErr != nil && oldErr.Error() != err.Error() {
			l.Infof("Folder %q error changed: %q -> %q", folder.ID, oldErr, err)
		} else if oldErr == nil {
			l.Warnf("Stopping folder %q - %v", folder.ID, err)
		}
		if runnerExists {
			runner.setError(err)
		}
	} else if oldErr != nil {
		l.Infof("Folder %q error is cleared, restarting", folder.ID)
		if runnerExists {
			runner.clearError()
		}
	}

	return err
}

func expandPath(path string) string {
	var err error

	// Attempt tilde expansion; leave unchanged in case of error
	path, _ = osutil.ExpandTilde(f.path)

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

func (f *Folder) CreateMarker() error {
	markerPath := filepath.Join(f.Path(), ".stfolder")
	fd, err := os.Create(marker)
	if err != nil {
		return err
	}
	fd.Close()
	osutil.HideFile(marker)

	return nil
}

func (f *Folder) HasMarker() bool {
	_, err := os.Stat(filepath.Join(f.Path(), ".stfolder"))
	return err == nil
}
