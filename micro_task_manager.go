package micro_task_pool

import (
	"errors"
	"sync"

	"container/list"

	logs "github.com/cihub/seelog"
	"github.com/psedison/tools"
)

var MicroTaskManager = &microTaskManager{}

type microTaskManager struct {
	bTaskExit     bool                      //队列退出
	curTaskLen    int                       //当前队列长度
	allTaskMap    map[string]*microTaskInfo //维护的所有go程队列 poolName=>microTaskInfo
	useShareQueue bool                      //是否所有task使用同一个队列， 默认使用同一个
}

type microTaskInfo struct {
	taskNum       int
	poolName      string
	taskQueueLen  int
	taskMap       map[int]MicroTaskInterface
	shareQueue    *MicroQueue //是否所有task使用同一个队列， 默认使用同一个
	taskList      *list.List  //用list保存 所有task，每次获取task 后都将改task放入队列末尾，下次取队列头的task，最大限度保证数据公平被task消费
	taskListMutex sync.Mutex
}

//Init 初始化,queueLen 队列长度， checkTimerInterval退款定时检查的间隔
func (this *microTaskManager) Init() error {
	logs.Info("balance manager init")
	this.allTaskMap = make(map[string]*microTaskInfo)
	this.useShareQueue = true //默认使用共享队列
	return nil
}

func (this *microTaskManager) UnInit() error {
	this.bTaskExit = true
	return nil
}

func (this *microTaskManager) setUseShareQueue(useShareQueue bool) error {
	this.useShareQueue = useShareQueue
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

		if this.useShareQueue {
			taskInfo.shareQueue = &MicroQueue{}
			taskInfo.shareQueue.Init(taskQueueLen)
			microTask.Init(poolName, i, taskQueueLen, handle, taskInfo.shareQueue)
		} else {
			microTask.Init(poolName, i, taskQueueLen, handle, nil)
		}

		microTask.Start()
		taskInfo.taskMap[i] = microTask
		taskInfo.taskList.PushBack(microTask) //将task放入 list
	}

	//go lib.WithRecover(this.accountCheckTask)
	this.bTaskExit = false
	return nil
}

func (this *microTaskManager) getPoolTaskInfo(poolName string) *microTaskInfo {
	if val, ok := this.allTaskMap[poolName]; !ok {
		taskInfo := &microTaskInfo{
			taskMap:  make(map[int]MicroTaskInterface),
			taskList: list.New(),
		}

		this.allTaskMap[poolName] = taskInfo
		return taskInfo
	} else {
		return val
	}
}

func (this *microTaskManager) PutQueue(poolName string, data interface{}, key string) error {

	if poolInfo, ok := this.allTaskMap[poolName]; ok {
		if this.useShareQueue {

			err := poolInfo.shareQueue.PutQueue(data, 2)
			if err != nil {
				logs.Warnf("micro task manager, put queue failed, pool name:%s, queue len:%d, data:%v", poolName, poolInfo.shareQueue.GetQueueLen(), data)
				return errors.New("put failed, timeout")
			}

			logs.Debugf("micro task manager, put queue success, pool name:%s, queue len:%d", poolName, poolInfo.shareQueue.GetQueueLen())
			return nil
		} else {
			task := poolInfo.getTask(key)
			if task == nil {
				logs.Warnf("micro task manager, put queue failed, pool name:%s, key:%s, error: get task is null", poolName, key)
				return errors.New("get task failed")
			}

			err := task.PutQueue(data)
			if err != nil {
				logs.Warnf("micro task manager, put queue failed, pool name:%s, task no:%d, error:%s", poolInfo.poolName, task.GetTaskNo(), err.Error())
				return err
			}

			logs.Debugf("micro task manager, put queue success, pool name:%s, task no:%d, task queue len:%d", poolInfo.poolName, task.GetTaskNo(), task.GetQueueLen())
		}
	} else {
		logs.Warnf("micro task manager, put queue failed, pool name:%s, error:pool name not exist", poolName)
		return errors.New("pool name not exist")
	}

	return nil
}

//getTask 根据key获取task， 若key为空 则获取空闲task
func (this *microTaskInfo) getTask(key string) MicroTaskInterface {
	if key != "" { //当传递了key则 根据key的hash值，来决定放入那个队列
		keyIndex := tools.RSHash(key) % this.taskNum
		return this.taskMap[keyIndex]
	}

	//若未传递key，则获取空闲的task
	return this.getIdleTask()
}

//getIdleTask 获取空闲go程。 空闲队列的定义：1、当前队列中无数据。2、队列中都有数据时，队列中数据最少的go程
func (this *microTaskInfo) getIdleTask() MicroTaskInterface {
	//1、先寻找最长时间未放入数据的task，若task中队列为空，则直接返回
	this.taskListMutex.Lock()
	defer this.taskListMutex.Unlock()
	if this.taskList.Len() <= 0 {
		return nil
	}

	leastQueueTask := this.taskList.Front()                                  //队列中数据最少的task
	leastQueueLen := leastQueueTask.Value.(MicroTaskInterface).GetQueueLen() //队列里面数据的长度
	for e := this.taskList.Front(); e != nil; e = e.Next() {
		if microTask, ok := e.Value.(MicroTaskInterface); ok {
			taskQueueLen := microTask.GetQueueLen()
			//如果task的队列里面没有数据，则直接放回该task
			if taskQueueLen <= 0 {
				leastQueueTask = e
				break
			} else {
				if leastQueueLen > taskQueueLen {
					leastQueueLen = taskQueueLen
					leastQueueTask = e
				}
			}
		}
	}

	//若第一个元素就是最优的队列，则将其移入最后，将其它空闲队列置顶 以便快速获取到
	if leastQueueTask == this.taskList.Front() {
		this.taskList.MoveToBack(leastQueueTask)
	} else {
		//若第一个元素不是最优队列，则将获取到的最优task移入list末尾， 再将第一个元素移入末尾（因第一个元素不是最优 将其移入末尾防止下次在从其开始遍历）
		// 让中间的空闲task 向上冒 以减少每次循环的检查的次数，也为了将长时间执行的task 经过几次遍历之后 移入到末尾
		this.taskList.MoveToBack(leastQueueTask)
		this.taskList.MoveToBack(this.taskList.Front()) //第一个元素移入末尾
	}

	return leastQueueTask.Value.(MicroTaskInterface)
}
