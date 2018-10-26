package micro_task_pool

import (
	"errors"
	"sync/atomic"

	logs "github.com/cihub/seelog"
	"github.com/psedison/tools"
)

var MicroTaskManager = &microTaskManager{}

type microTaskManager struct {
	bTaskExit  bool                      //队列退出
	curTaskLen int                       //当前队列长度
	putTaskNum uint64                    //当前推入了队列的数量，用于均衡的向队列中放入数据
	allTaskMap map[string]*microTaskInfo //维护的所有go程队列 poolName=>microTaskInfo
}

type microTaskInfo struct {
	taskNum      int
	poolName     string
	taskQueueLen int
	taskMap      map[int]MicroTaskInterface
}

//Init 初始化,queueLen 队列长度， checkTimerInterval退款定时检查的间隔
func (this *microTaskManager) Init() error {
	logs.Info("balance manager init")
	this.allTaskMap = make(map[string]*microTaskInfo)
	return nil
}

func (this *microTaskManager) UnInit() error {
	this.bTaskExit = true
	return nil
}

//StartTaskPool 启动处理go程池, poolName:任务名称， pollTaskNum:任务数量, taskQueueLen:单个go程内的队列数量
func (this *microTaskManager) StartTaskPool(poolName string, pollTaskNum int, taskQueueLen int32, handle ProcessHandle) error {
	logs.Infof("micro task manager start, pool name:%s, task num:%s, task queue len:%d", poolName, pollTaskNum, taskQueueLen)

	taskInfo := this.getPoolTaskInfo(poolName)
	taskInfo.taskNum = pollTaskNum
	taskInfo.poolName = poolName

	for i := 0; i < pollTaskNum; i++ {
		logs.Infof("micro task manager start, pool name:%s, task num:%d, task queue len:%d, init task, task no:%d", poolName, pollTaskNum, taskQueueLen, i)
		microTask := &MicroTask{}

		microTask.Init(poolName, i, taskQueueLen, handle)
		microTask.Start()
		taskInfo.taskMap[i] = microTask
	}

	//go lib.WithRecover(this.accountCheckTask)
	this.bTaskExit = false
	return nil
}

func (this *microTaskManager) getPoolTaskInfo(poolName string) *microTaskInfo {
	if val, ok := this.allTaskMap[poolName]; !ok {
		taskInfo := &microTaskInfo{
			taskMap: make(map[int]MicroTaskInterface),
		}

		this.allTaskMap[poolName] = taskInfo
		return taskInfo
	} else {
		return val
	}
}

func (this *microTaskManager) PutQueue(poolName string, data interface{}, key string) error {

	if poolInfo, ok := this.allTaskMap[poolName]; ok {
		keyIndex := 0
		if key != "" { //当传递了key则 根据key的hash值，来决定放入那个队列
			keyIndex = tools.RSHash(key) % poolInfo.taskNum
		} else { // 否则轮训
			newNum := atomic.AddUint64(&this.putTaskNum, 1)   //将数量+1，得到新值
			keyIndex = int(newNum % uint64(poolInfo.taskNum)) //取模获取 队列编号
		}

		task := poolInfo.taskMap[keyIndex]
		err := task.PutQueue(data)
		if err != nil {
			logs.Warnf("micro task manager, put queue failed, pool name:%s, task no:%d, error:%s", poolInfo.poolName, keyIndex, err.Error())
			return err
		}
		logs.Debugf("micro task manager, put queue success, pool name:%s, task no:%d", poolInfo.poolName, keyIndex)
	} else {
		logs.Warnf("micro task manager, put queue failed, pool name:%s, error:pool name not exist", poolName)
		return errors.New("pool name not exist")
	}

	return nil
}
