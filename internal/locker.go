package internal

import "sync"

type Locker sync.Mutex

func (l Locker) Lock(f func()) {
	var mutex sync.Mutex = sync.Mutex(l)
	mutex.Lock()
	defer mutex.Unlock()

	f()
}
