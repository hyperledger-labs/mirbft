package logging

import "sync"

type synchronizedLogger struct {
	logger Logger
	mutex  sync.Mutex
}

func (sl *synchronizedLogger) Log(level LogLevel, text string, args ...interface{}) {
	sl.mutex.Lock()
	sl.logger.Log(level, text, args...)
	sl.mutex.Unlock()
}

func Synchronize(logger Logger) Logger {
	return &synchronizedLogger{
		logger: logger,
	}
}
