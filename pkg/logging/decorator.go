package logging

import "fmt"

type decoratedLogger struct {
	logger Logger
	prefix string
	args   []interface{}
}

func (dl *decoratedLogger) Log(level LogLevel, text string, args ...interface{}) {
	passedArgs := append(dl.args, args...)
	dl.logger.Log(level, fmt.Sprintf("%s%s", dl.prefix, text), passedArgs...)
}

func Decorate(logger Logger, prefix string, args ...interface{}) Logger {
	return &decoratedLogger{
		prefix: prefix,
		logger: logger,
		args:   args,
	}
}
