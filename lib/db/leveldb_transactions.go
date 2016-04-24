// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package db

import (
	"bytes"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syndtr/goleveldb/leveldb"
)

// A readOnlyTransaction represents a database snapshot.
type readOnlyTransaction struct {
	*leveldb.Snapshot
	db *Instance
}

func (db *Instance) newReadOnlyTransaction() readOnlyTransaction {
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	return readOnlyTransaction{
		Snapshot: snap,
		db:       db,
	}
}

func (t readOnlyTransaction) close() {
	t.Release()
}

func (t readOnlyTransaction) getFile(folder, device, file []byte) (protocol.FileInfo, bool) {
	return getFile(t, t.db.deviceKey(folder, device, file))
}

// A readWriteTransaction is a readOnlyTransaction plus a batch for writes.
// The batch will be committed on close() or by checkFlush() if it exceeds the
// batch size.
type readWriteTransaction struct {
	readOnlyTransaction
	*leveldb.Batch
}

func (db *Instance) newReadWriteTransaction() readWriteTransaction {
	t := db.newReadOnlyTransaction()
	return readWriteTransaction{
		readOnlyTransaction: t,
		Batch:               new(leveldb.Batch),
	}
}

func (t readWriteTransaction) close() {
	if err := t.db.Write(t.Batch, nil); err != nil {
		panic(err)
	}
	t.readOnlyTransaction.close()
}

func (t readWriteTransaction) checkFlush() {
	if t.Batch.Len() > batchFlushSize {
		if err := t.db.Write(t.Batch, nil); err != nil {
			panic(err)
		}
		t.Batch.Reset()
	}
}

func (t readWriteTransaction) insertFile(folder, device []byte, file protocol.FileInfo) int64 {
	l.Debugf("insert; folder=%q device=%v %v", folder, protocol.DeviceIDFromBytes(device), file)

	if file.LocalVersion == 0 {
		file.LocalVersion = clock(0)
	}

	name := []byte(file.Name)
	nk := t.db.deviceKey(folder, device, name)
	t.Put(nk, file.MustMarshalXDR())

	return file.LocalVersion
}

// updateGlobal adds this device+version to the version list for the given
// file. If the device is already present in the list, the version is updated.
// If the file does not have an entry in the global list, it is created.
func (t readWriteTransaction) updateGlobal(folder, device []byte, file protocol.FileInfo, size *sizeTracker) bool {
	l.Debugf("update global; folder=%q device=%v file=%q version=%d", folder, protocol.DeviceIDFromBytes(device), file.Name, file.Version)
	name := []byte(file.Name)
	gk := t.db.globalKey(folder, name)
	svl, err := t.Get(gk, nil)
	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	// We unmarshal the existing version list into oldVersionList and keep it
	// unchanged, and make a copy into newVersionList that we'll later
	// modify.
	var newVersionList, oldVersionList versionList
	if len(svl) != 0 {
		err = oldVersionList.UnmarshalXDR(svl)
		if err != nil {
			panic(err)
		}
		newVersionList.versions = make([]fileVersion, len(oldVersionList.versions))
		copy(newVersionList.versions, oldVersionList.versions)
	}

	// Remove the entry for the device from the new version list, so we can
	// add the new entry.
	for i := range newVersionList.versions {
		if bytes.Equal(newVersionList.versions[i].device, device) {
			if newVersionList.versions[i].version.Equal(file.Version) {
				// No need to do anything, the version list already contains
				// exactly the one we were going to add.
				return false
			}

			newVersionList.versions = append(newVersionList.versions[:i], newVersionList.versions[i+1:]...)
			break
		}
	}

	nv := fileVersion{
		device:  device,
		version: file.Version,
	}

	// Find a position in the list to insert this file. The file at the front
	// of the list is the newer, the "global".
	for i := range newVersionList.versions {
		switch newVersionList.versions[i].version.Compare(file.Version) {
		case protocol.Equal, protocol.Lesser:
			// The version at this point in the list is equal to or lesser
			// ("older") than us. We insert ourselves in front of it.
			newVersionList.versions = insertVersion(newVersionList.versions, i, nv)
			goto done

		case protocol.ConcurrentLesser, protocol.ConcurrentGreater:
			// The version at this point is in conflict with us. We must pull
			// the actual file metadata to determine who wins. If we win, we
			// insert ourselves in front of the loser here. (The "Lesser" and
			// "Greater" in the condition above is just based on the device
			// IDs in the version vector, which is not the only thing we use
			// to determine the winner.)
			of, ok := t.getFile(folder, newVersionList.versions[i].device, name)
			if !ok {
				panic("file referenced in version list does not exist")
			}
			if file.WinsConflict(of) {
				newVersionList.versions = insertVersion(newVersionList.versions, i, nv)
				goto done
			}
		}
	}

	// We didn't find a position for an insert above, so append to the end.
	newVersionList.versions = append(newVersionList.versions, nv)

done:
	l.Debugf("new global after update: %v", newVersionList)
	t.Put(gk, newVersionList.MustMarshalXDR())

	t.updateGlobalSizeFixup(oldVersionList.versions, newVersionList.versions, folder, device, name, file, size)
	return true
}

func (t readWriteTransaction) updateGlobalSizeFixup(oldV, newV []fileVersion, folder, device, name []byte, file protocol.FileInfo, size *sizeTracker) {
	if len(oldV) == 0 {
		// A new file was added. It's in sync by definition.
		size.insync(device).addFile(file)
		size.global.addFile(file)
		return
	}

	oldGlobalVersion := oldV[0].version
	newGlobalVersion := newV[0].version

	if oldGlobalVersion.Equal(newGlobalVersion) {
		// The global version didn't change. We just need to handle the
		// thing that was added.
		if oldGlobalVersion.Equal(file.Version) {
			// The file that was added has the same version as the global
			// version - it's in sync.
			size.insync(device).addFile(file)
		}
		return
	}

	oldGlobal, ok := t.getFile(folder, oldV[0].device, name)
	if !ok {
		panic("replacing non-existant file")
	}

	// The files that were previously accounted as in sync are at the head
	// of the old version list, with version == oldGlobalVersion. Deduct
	// those sizes.
	for _, v := range oldV {
		if v.version.Equal(oldGlobalVersion) {
			size.insync(v.device).removeFile(oldGlobal)
		} else {
			break
		}
	}

	// The files that are now in sync are at the head of the new version
	// list, with version == newGlobalVersion. Add those sizes.
	for _, v := range newV {
		if v.version.Equal(newGlobalVersion) {
			size.insync(v.device).addFile(file)
		} else {
			break
		}
	}

	size.global.removeFile(oldGlobal)
	size.global.addFile(file)
}

// removeFromGlobal removes the device from the global version list for the
// given file. If the version list is empty after this, the file entry is
// removed entirely.
func (t readWriteTransaction) removeFromGlobal(folder, device, name []byte, size *sizeTracker) {
	l.Debugf("remove from global; folder=%q device=%v name=%q", folder, protocol.DeviceIDFromBytes(device), name)

	gk := t.db.globalKey(folder, name)
	svl, err := t.Get(gk, nil)
	if err != nil {
		// We might be called to "remove" a global version that doesn't exist
		// if the first update for the file is already marked invalid.
		return
	}

	var oldVersions, newVersions versionList
	err = oldVersions.UnmarshalXDR(svl)
	if err != nil {
		panic(err)
	}
	newVersions.versions = make([]fileVersion, len(oldVersions.versions))
	copy(newVersions.versions, oldVersions.versions)

	for i := range newVersions.versions {
		if bytes.Equal(newVersions.versions[i].device, device) {
			newVersions.versions = append(newVersions.versions[:i], newVersions.versions[i+1:]...)
			break
		}
	}

	if len(newVersions.versions) == 0 {
		t.Delete(gk)
	} else {
		l.Debugf("new global after remove: %v", newVersions)
		t.Put(gk, newVersions.MustMarshalXDR())
	}

	t.removeGlobalSizeFixup(oldVersions.versions, newVersions.versions, folder, device, name, size)
}

func (t readWriteTransaction) removeGlobalSizeFixup(oldV, newV []fileVersion, folder, device, name []byte, size *sizeTracker) {
	if len(oldV) == 0 {
		return
	}

	// If we had one version previously, it was the global version.
	// Account it as removed.
	oldGlobal, ok := t.getFile(folder, oldV[0].device, name)
	if !ok {
		panic("replacing non-existant file")
	}

	if len(newV) == 0 {
		// The last version was removed.
		size.insync(device).removeFile(oldGlobal)
		size.global.removeFile(oldGlobal)
		return
	}

	oldGlobalVersion := oldV[0].version
	newGlobalVersion := newV[0].version

	var removedVersion protocol.Vector
	for _, v := range oldV {
		if bytes.Equal(v.device, device) {
			removedVersion = v.version
			break
		}
	}

	if oldGlobalVersion.Equal(newGlobalVersion) {
		// The global version didn't change. We just need to handle the
		// thing that was removed.
		if oldGlobalVersion.Equal(removedVersion) {
			// The file that was removed has the same version as the global
			// version - it was in sync.
			size.insync(device).removeFile(oldGlobal)
		}
		return
	}

	// The files that were previously accounted as in sync are at the head
	// of the old version list, with version == oldGlobalVersion. Deduct
	// those sizes.
	for _, v := range oldV {
		if v.version.Equal(oldGlobalVersion) {
			size.insync(v.device).removeFile(oldGlobal)
		} else {
			break
		}
	}

	// The files that are now in sync are at the head of the new version
	// list, with version == newGlobalVersion. Add those sizes.
	newGlobal, ok := t.getFile(folder, newV[0].device, name)
	if !ok {
		panic("promoting non-existant file")
	}
	for _, v := range newV {
		if v.version.Equal(newGlobalVersion) {
			size.insync(v.device).addFile(newGlobal)
		} else {
			break
		}
	}

	size.global.removeFile(oldGlobal)
	size.global.addFile(newGlobal)
}

func insertVersion(vl []fileVersion, i int, v fileVersion) []fileVersion {
	t := append(vl, fileVersion{})
	copy(t[i+1:], t[i:])
	t[i] = v
	return t
}
