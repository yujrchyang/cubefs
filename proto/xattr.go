// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package proto

const (
	XATTR_FLOCK = "cfs_flock"
)

const (
	// if a dir has FLAG_RECURSIVE, all sub files and dirs have the same flags of the dir
	XATTR_FLOCK_FLAG_RECURSIVE = 1 << iota
	XATTR_FLOCK_FLAG_WRITE
)

type XAttrFlock struct {
	Version   uint8  `json:"v"`
	WaitTime  uint8  `json:"wt"`
	Flag      uint16 `json:"flg"`
	IpPort    string `json:"ip"` // ip:port
	ValidTime uint64 `json:"vt"`
}
