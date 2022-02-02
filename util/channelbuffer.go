package util

import (
	"time"

	logger "github.com/rs/zerolog/log"
)

const (
	inputChannelBufferSize = 1024
)

type ChannelBuffer struct {
	capacity     int
	buffer       []interface{}
	inputChan    chan interface{}
	outputChan   chan []interface{}
	stopFuncChan chan struct{}
}

func NewChannelBuffer(capacity int) *ChannelBuffer {
	cb := ChannelBuffer{
		capacity:     capacity,
		buffer:       make([]interface{}, 0, capacity),
		inputChan:    make(chan interface{}, inputChannelBufferSize),
		outputChan:   make(chan []interface{}),
		stopFuncChan: make(chan struct{}),
	}

	go cb.processInput()

	return &cb
}

func (cb *ChannelBuffer) processInput() {
	for item := range cb.inputChan {
		if item != nil {
			if cb.capacity == 0 || len(cb.buffer) < cb.capacity {
				cb.buffer = append(cb.buffer, item)
			} else {
				logger.Warn().
					Int("capacity", cb.capacity).
					Msgf("Channel buffer capacity exceeded. Ignoring value: %v", item)
			}
		} else {
			cb.outputChan <- cb.buffer
			cb.buffer = make([]interface{}, 0, cb.capacity)
		}
	}
	cb.outputChan <- cb.buffer
}

// ATTENTION: nil must not be passed as an argument to Add().
func (cb *ChannelBuffer) Add(item interface{}) {
	cb.inputChan <- item
}

func (cb *ChannelBuffer) Get() []interface{} {
	cb.inputChan <- nil
	return <-cb.outputChan
}

func (cb *ChannelBuffer) PeriodicFunc(period time.Duration, f func([]interface{})) {
	go func() {
		for {
			f(cb.Get())
			select {
			case <-cb.stopFuncChan:
				cb.stopFuncChan <- struct{}{}
				return
			default:
				time.Sleep(period)
			}
		}
	}()
}

func (cb *ChannelBuffer) StopFunc() {
	cb.stopFuncChan <- struct{}{}
	<-cb.stopFuncChan
}

// ATTENTION: Add() or Get() must not be called after or concurrently with Close().
func (cb *ChannelBuffer) Close() []interface{} {
	close(cb.inputChan)
	return <-cb.outputChan
}
