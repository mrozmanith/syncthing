// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package db

import (
	"encoding/binary"
	"fmt"

	"github.com/calmh/xdr"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var blockFinder *BlockFinder

const maxBatchSize = 256 << 10

type BlockMap struct {
	db       *Instance
	folderID uint32
}

func NewBlockMap(db *Instance, folderID uint32) *BlockMap {
	return &BlockMap{
		db:       db,
		folderID: folderID,
	}
}

// Add files to the block map, ignoring any deleted or invalid files.
func (m *BlockMap) Add(files []protocol.FileInfo) error {
	batch := new(leveldb.Batch)
	key := make([]byte, 33)
	key[0] = KeyTypeBlock
	var buf []byte
	for _, file := range files {
		if batch.Len() > maxBatchSize {
			if err := m.db.Write(batch, nil); err != nil {
				return err
			}
			batch.Reset()
		}

		if file.IsDirectory() || file.IsDeleted() || file.IsInvalid() {
			continue
		}

		nameID := m.db.nameIdx.ID([]byte(file.Name))
		for i, block := range file.Blocks {
			copy(key[1:], block.Hash)
			var bl blockmapList
			bs, err := m.db.Get(key, nil)
			if err == nil {
				bl.UnmarshalXDR(bs)
			}
			bl.locations = append(bl.locations, blockmapLocation{
				folderID: m.folderID,
				nameID:   nameID,
				blockIdx: uint32(i),
			})
			reqLen := bl.XDRSize()
			if len(buf) < reqLen {
				buf = make([]byte, reqLen)
			}
			bl.MarshalXDRInto(&xdr.Marshaller{Data: buf})
			batch.Put(key, buf[:reqLen])
		}
	}
	return m.db.Write(batch, nil)
}

// Update block map state, removing any deleted or invalid files.
func (m *BlockMap) Update(files []protocol.FileInfo) error {
	batch := new(leveldb.Batch)
	key := make([]byte, 33)
	key[0] = KeyTypeBlock
	var buf []byte
	for _, file := range files {
		if batch.Len() > maxBatchSize {
			if err := m.db.Write(batch, nil); err != nil {
				return err
			}
			batch.Reset()
		}

		if file.IsDirectory() {
			continue
		}

		if file.IsDeleted() || file.IsInvalid() {
			for _, block := range file.Blocks {
				key = m.blockKeyInto(key, block.Hash, file.Name)
				batch.Delete(key)
			}
			continue
		}

		nameID := m.db.nameIdx.ID([]byte(file.Name))
		for i, block := range file.Blocks {
			copy(key[1:], block.Hash)
			var bl blockmapList
			bs, err := m.db.Get(key, nil)
			if err == nil {
				bl.UnmarshalXDR(bs)
			}

			for i := range bl.locations {
				if bl.locations[i].nameID == nameID && bl.locations[i].folderID == m.folderID {
					copy(bl.locations[i:], bl.locations[i+1:])
					bl.locations = bl.locations[:len(bl.locations)-1]
					break
				}
			}

			reqLen := bl.XDRSize()
			if len(buf) < reqLen {
				buf = make([]byte, reqLen)
			}
			bl.MarshalXDRInto(&xdr.Marshaller{Data: buf})
			batch.Put(key, buf[:reqLen])
		}
	}
	return m.db.Write(batch, nil)
}

// Discard block map state, removing the given files
func (m *BlockMap) Discard(files []protocol.FileInfo) error {
	batch := new(leveldb.Batch)
	var key []byte
	for _, file := range files {
		if batch.Len() > maxBatchSize {
			if err := m.db.Write(batch, nil); err != nil {
				return err
			}
			batch.Reset()
		}

		for _, block := range file.Blocks {
			key = m.blockKeyInto(key, block.Hash, file.Name)
			batch.Delete(key)
		}
	}
	return m.db.Write(batch, nil)
}

// Drop block map, removing all entries related to this block map from the db.
func (m *BlockMap) Drop() error {
	batch := new(leveldb.Batch)
	iter := m.db.NewIterator(util.BytesPrefix(m.blockKeyInto(nil, nil, "")[:keyPrefixLen+keyFolderLen]), nil)
	defer iter.Release()
	for iter.Next() {
		if batch.Len() > maxBatchSize {
			if err := m.db.Write(batch, nil); err != nil {
				return err
			}
			batch.Reset()
		}

		batch.Delete(iter.Key())
	}
	if iter.Error() != nil {
		return iter.Error()
	}
	return m.db.Write(batch, nil)
}

type BlockFinder struct {
	db *Instance
}

func NewBlockFinder(db *Instance) *BlockFinder {
	if blockFinder != nil {
		return blockFinder
	}

	f := &BlockFinder{
		db: db,
	}

	return f
}

func (f *BlockFinder) String() string {
	return fmt.Sprintf("BlockFinder@%p", f)
}

// Iterate takes an iterator function which iterates over all matching blocks
// for the given hash. The iterator function has to return either true (if
// they are happy with the block) or false to continue iterating for whatever
// reason. The iterator finally returns the result, whether or not a
// satisfying block was eventually found.
func (f *BlockFinder) Iterate(folders []string, hash []byte, iterFn func(string, string, int32) bool) bool {
	var key []byte
	for _, folder := range folders {
		folderID := f.db.folderIdx.ID([]byte(folder))
		key = blockKeyInto(key, hash, folderID, "")
		iter := f.db.NewIterator(util.BytesPrefix(key), nil)
		defer iter.Release()

		for iter.Next() && iter.Error() == nil {
			file := blockKeyName(iter.Key())
			index := int32(binary.BigEndian.Uint32(iter.Value()))
			if iterFn(folder, osutil.NativeFilename(file), index) {
				return true
			}
		}
	}
	return false
}

// Fix repairs incorrect blockmap entries, removing the old entry and
// replacing it with a new entry for the given block
func (f *BlockFinder) Fix(folder, file string, index int32, oldHash, newHash []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(index))

	folderID := f.db.folderIdx.ID([]byte(folder))
	batch := new(leveldb.Batch)
	batch.Delete(blockKeyInto(nil, oldHash, folderID, file))
	batch.Put(blockKeyInto(nil, newHash, folderID, file), buf)
	return f.db.Write(batch, nil)
}

// m.blockKey returns a byte slice encoding the following information:
//	   keyTypeBlock (1 byte)
//	   folder (4 bytes)
//	   block hash (32 bytes)
//	   file name (variable size)
func blockKeyInto(o, hash []byte, folder uint32, file string) []byte {
	reqLen := keyPrefixLen + keyFolderLen + keyHashLen + len(file)
	if cap(o) < reqLen {
		o = make([]byte, reqLen)
	} else {
		o = o[:reqLen]
	}
	o[0] = KeyTypeBlock
	binary.BigEndian.PutUint32(o[keyPrefixLen:], folder)
	copy(o[keyPrefixLen+keyFolderLen:], []byte(hash))
	copy(o[keyPrefixLen+keyFolderLen+keyHashLen:], []byte(file))
	return o
}

// blockKeyName returns the file name from the block key
func blockKeyName(data []byte) string {
	if len(data) < keyPrefixLen+keyFolderLen+keyHashLen+1 {
		panic("Incorrect key length")
	}
	if data[0] != KeyTypeBlock {
		panic("Incorrect key type")
	}

	file := string(data[keyPrefixLen+keyFolderLen+keyHashLen:])
	return file
}
