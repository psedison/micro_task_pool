package micro_task_pool

import (
	logs "github.com/cihub/seelog"
	"github.com/psedison/tools"
)

type MicroTaskInterface interface {
	Init(poolName string, taskNo int, queueLen int32, handle ProcessHandle, shareQueue *MicroQueue) error
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
	transferQueue *MicroQueue   //退款队列，所有退款请求加入队列，并行化处理
	bTaskExit     bool          //队列退出
	queueMaxLen   int32         //退款队列最大长度, 容量超过，则会阻塞
	curQueueLen   int32         //当前队列长度
	taskName      string        //
	taskNo        int           //go程的编号
	handle        ProcessHandle //处理数据的Handel
	useShareQueue bool
}

func (this *MicroTask) Init(taskName string, taskNo int, queueLen int32, handle ProcessHandle, shareQueue *MicroQueue) error {
	logs.Infof("micro task init, task name:%s, task no:%d, queue len:%d", taskName, taskNo, queueLen)
	this.taskNo = taskNo
	this.taskName = taskName
	if shareQueue != nil { //参数不为空，则表示所有task使用同一个队列
		this.transferQueue = shareQueue
		this.useShareQueue = true
	} else {
		this.transferQueue = &MicroQueue{}
		this.transferQueue.Init(queueLen)
		this.useShareQueue = false
	}

	this.queueMaxLen = queueLen
	this.handle = handle
	logs.Infof("micro task init success, task name:%s, task no:%d, queue len:%d", taskName, taskNo, queueLen)
	return nil
}

func (this *MicroTask) UnInit() error {
	this.bTaskExit = true
	if !this.useShareQueue {
		this.transferQueue.UnInit()
	}

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
	return this.transferQueue.PutQueue(data, 2)
}

func (this *MicroTask) runTask() {
	logs.Infof("micro task, running, task name:%s, task no:%d", this.taskName, this.taskNo)
	for !this.bTaskExit {
		data, err := this.transferQueue.GetQueue(60)
		if err != nil {
			logs.Debugf("micro task check is running, get queue failed, task name:%s, task no:%d, error:%s", this.taskName, this.taskNo, err.Error())
			continue
		}

		this.ProcessData(data)
	}
}

func (this *MicroTask) ProcessData(data interface{}) error {
	if this.handle == nil {
		logs.Warnf("micro task, process data failed.")
		return nil
	}

	logs.Debugf("micro task, process data begin, task name:%s, task no:%d", this.taskName, this.taskNo)
	this.handle.ProcessData(data)
	return nil
}
