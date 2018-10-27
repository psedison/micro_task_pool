package micro_task_pool

import (
	"errors"
	"sync/atomic"
	"time"

	logs "github.com/cihub/seelog"
	"github.com/psedison/tools"
)

type MicroTaskInterface interface {
	Init(poolName string, taskNo int, queueLen int32, handle ProcessHandle) error
	UnInit() error
	Start() error
	PutQueue(data interface{}) error
	GetQueueLen() int32
	GetTaskNo() int
}

type ProcessHandle interface {
	ProcessData(data interface{}) error
}

type MicroTask struct {
	transferQueue chan interface{} //退款队列，所有退款请求加入队列，并行化处理
	bTaskExit     bool             //队列退出
	queueMaxLen   int32            //退款队列最大长度, 容量超过，则会阻塞
	curQueueLen   int32            //当前队列长度
	taskName      string           //
	taskNo        int              //go程的编号
	handle        ProcessHandle    //处理数据的Handel
}

func (this *MicroTask) Init(taskName string, taskNo int, queueLen int32, handle ProcessHandle) error {
	logs.Infof("micro task init, task name:%s, task no:%d, queue len:%d", taskName, taskNo, queueLen)
	this.taskNo = taskNo
	this.taskName = taskName
	this.transferQueue = make(chan interface{}, queueLen)
	this.queueMaxLen = queueLen
	this.handle = handle
	logs.Infof("micro task init success, task name:%s, task no:%d, queue len:%d", taskName, taskNo, queueLen)
	return nil
}

func (this *MicroTask) UnInit() error {
	this.bTaskExit = true
	close(this.transferQueue)
	return nil
}

//启动处理转账的go程
func (this *MicroTask) Start() error {
	logs.Infof("micro task start, task no:%d, queue len:%d", this.taskNo, this.queueMaxLen)
	go tools.WithRecover(this.runTask)
	this.bTaskExit = false
	logs.Infof("micro task start success, task no:%d, queue len:%d", this.taskNo, this.queueMaxLen)
	return nil
}

//GetTaskNo 获取队列编号
func (this *MicroTask) GetTaskNo() int {
	return this.taskNo
}

//GetQueueLen 获取队列长度
func (this *MicroTask) GetQueueLen() int32 {
	return this.curQueueLen
}

func (this *MicroTask) PutQueue(data interface{}) error {
	after := time.NewTimer(time.Second * 2)
	defer after.Stop()

	select {
	case <-after.C: //超时处理， 因往chan中放入数据时，如果队列满了，则会阻塞  为防止将程序阻塞，故此处设置2秒超时，超时则本次操作失败，等待下次定时检查在执行
		logs.Warnf("micro task, put queue failed, task no:%d, data:%v", this.taskNo, data)
		return errors.New("put failed, timeout")
	case this.transferQueue <- data:
		atomic.AddInt32(&this.curQueueLen, 1)
	}

	return nil
}

func (this *MicroTask) runTask() {
	logs.Infof("micro task, running, task name:%s, task no:%d", this.taskName, this.taskNo)
	t := time.NewTimer(time.Second * time.Duration(60)) //每60秒钟唤醒一次，打印日志
	for !this.bTaskExit {
		select {
		case data := <-this.transferQueue:
			this.ProcessData(data)
			atomic.AddInt32(&this.curQueueLen, -1) //将队列长度-1
		case <-t.C:
			logs.Infof("micro task check is running, task name:%s, task no:%d", this.taskName, this.taskNo)
			t.Reset(time.Second * time.Duration(60))
		}
	}
}

func (this *MicroTask) ProcessData(data interface{}) error {
	if this.handle == nil {
		logs.Warnf("micro task, process data failed.")
		return nil
	}

	this.handle.ProcessData(data)
	return nil
}
