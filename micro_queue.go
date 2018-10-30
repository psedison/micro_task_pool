package micro_task_pool

import (
	"errors"
	"sync/atomic"
	"time"
)

type MicroQueue struct {
	transferQueue    chan interface{}
	transferQueueLen int32
}

func (this *MicroQueue) Init(queueLen int32) error {
	this.transferQueue = make(chan interface{}, queueLen)
	return nil
}

func (this *MicroQueue) UnInit() error {
	if this.transferQueue != nil {
		close(this.transferQueue)
	}

	return nil
}

func (this *MicroQueue) PutQueue(data interface{}, timeout int32) error {
	if timeout >= 0 {
		after := time.NewTimer(time.Second * time.Duration(timeout))
		defer after.Stop()

		select {
		case <-after.C: //超时处理， 因往chan中放入数据时，如果队列满了，则会阻塞  为防止将程序阻塞，故此处设置2秒超时，超时则本次操作失败，等待下次定时检查在执行
			return errors.New("put failed, timeout")
		case this.transferQueue <- data:
			atomic.AddInt32(&this.transferQueueLen, 1)
		}
	} else {
		this.transferQueue <- data
	}

	return nil
}

func (this *MicroQueue) GetQueue(timeout int32) (interface{}, error) {
	if timeout >= 0 {
		after := time.NewTimer(time.Second * time.Duration(timeout)) //每60秒钟唤醒一次，打印日志
		defer after.Stop()

		select {
		case data := <-this.transferQueue:
			atomic.AddInt32(&this.transferQueueLen, -1) //将队列长度-1
			return data, nil
		case <-after.C:
			return nil, errors.New("get queue timeout")
		}

	} else {
		data := <-this.transferQueue
		return data, nil
	}
}

func (this *MicroQueue) GetQueueLen() int32 {
	return this.transferQueueLen
}
