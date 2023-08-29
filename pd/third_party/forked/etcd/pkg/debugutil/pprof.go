// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package debugutil

import (
	"net/http"
	"net/http/pprof"
	"runtime"
)

// PProfHandlers returns a map of pprof handlers keyed by the HTTP path.
func PProfHandlers(prefix string) map[string]http.Handler {
	// set only when there's no existing setting
	if runtime.SetMutexProfileFraction(-1) == 0 {
		// 1 out of 5 mutex events are reported, on average
		runtime.SetMutexProfileFraction(5)
	}

	m := make(map[string]http.Handler)

	m[prefix+"/"] = http.HandlerFunc(pprof.Index)
	m[prefix+"/profile"] = http.HandlerFunc(pprof.Profile)
	m[prefix+"/symbol"] = http.HandlerFunc(pprof.Symbol)
	m[prefix+"/cmdline"] = http.HandlerFunc(pprof.Cmdline)
	m[prefix+"/trace"] = http.HandlerFunc(pprof.Trace)
	m[prefix+"/heap"] = pprof.Handler("heap")
	m[prefix+"/goroutine"] = pprof.Handler("goroutine")
	m[prefix+"/threadcreate"] = pprof.Handler("threadcreate")
	m[prefix+"/block"] = pprof.Handler("block")
	m[prefix+"/mutex"] = pprof.Handler("mutex")

	return m
}
