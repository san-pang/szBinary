package szBinary

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

// openOrCreateFile opens a file for reading and writing, creating it if necessary
func openOrCreateFile(fname string, perm os.FileMode) (f *os.File, err error) {
	dir := filepath.Dir(fname)
	if _, err = os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dir, perm); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	if f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, perm); err != nil {
		return nil, fmt.Errorf("error opening or creating file: %s: %s", fname, err.Error())
	}
	return f, nil
}

// closeFile behaves like Close, except that no error is returned if the file does not exist
func closeFile(f *os.File) error {
	if f != nil {
		if err := f.Close(); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// removeFile behaves like os.Remove, except that no error is returned if the file does not exist
func removeFile(fname string) error {
	if err := os.Remove(fname); (err != nil) && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %v", fname)
	}
	return nil
}