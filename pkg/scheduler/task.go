package scheulder

import (
	"encoding/json"
	"errors"
	"golanglearning/new_project/cron-task-demo/pkg/redis"
	"log"
	"time"
)

// CronTaskManager define bucket ticker
type CronTaskManager struct {
	redisClient *redis.Client
	Ticker      *time.Ticker
	Interval    time.Duration
	Name        string
	ExecuteFunc func(interface{}) bool
	stopC       chan struct{}
}

const (
	CronType = "cron"
	OnceType = "once"
)

// Task task
type Task struct {
	Id        string //task id global uniqueness
	Type      string
	Data      interface{}   //data of task
	Delay     time.Duration //delay time, 30 means after 30 second
	Timestamp int
}

// NewCronTaskManager new ticker
func NewCronTaskManager(interval time.Duration, managerName string, executeFunc func(interface{}) bool, client *redis.Client) (*CronTaskManager, error) {
	if interval <= 0 || executeFunc == nil {
		return nil, errors.New("create cron task ticker manager fail")
	}
	manager := &CronTaskManager{
		Interval:    interval,
		Name:        managerName,
		ExecuteFunc: executeFunc,
		redisClient: client,
	}
	return manager, nil
}

func (ctm *CronTaskManager) addTaskForCron(task *Task) error {

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	var copy Task
	err = json.Unmarshal(data, &copy)
	if err != nil {
		return err
	}

	err = ctm.addTask(&copy)
	if err != nil {
		log.Println("error happen!", err)
		return err
	}
	return nil
}

func (ctm *CronTaskManager) AddTask(task *Task) error {
	return ctm.addTask(task)
}

// add task
func (ctm *CronTaskManager) addTask(task *Task) error {
	// 延迟时间 zadd
	timestamp := time.Now().Add(task.Delay).Unix()
	err := ctm.redisClient.ZAdd(ctm.Name, int(timestamp), task.Id)
	if err != nil {
		return err
	}

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	err = ctm.redisClient.Set(task.Id, string(data))
	if err != nil {
		return err
	}
	return nil
}

// Start 启动定时器
func (ctm *CronTaskManager) Start() {
	timer := time.NewTicker(ctm.Interval) //interval
	go func() {
		for {
			select {
			case t := <-timer.C:
				ctm.tickHandler(t, ctm.Name)
			case <-ctm.stopC:
				return
			}
		}
	}()
}

func (ctm *CronTaskManager) Stop() {
	defer ctm.redisClient.Close()
	ctm.stopC <- struct{}{}
}

// tick handler
func (ctm *CronTaskManager) tickHandler(currentTime time.Time, bucketName string) {
	for {
		task, err := ctm.getTask(bucketName)
		if err != nil {
			log.Println("error happen!", err)
			return
		}
		// 没有任务时跳过
		if task == nil {
			return
		}

		if task.Timestamp > int(currentTime.Unix()) {
			return
		}
		// do task
		taskDetail, err := ctm.getTaskById(task)
		if err != nil {
			log.Println("error happen!", err)
			continue
		}
		// 执行任务
		if ok := ctm.ExecuteFunc(taskDetail.Data); ok {
			// 执行后区分不同类型任务
			switch task.Type {
			case CronType:
				err = ctm.addTaskForCron(task)
				if err != nil {
					continue
				}
			case OnceType:
				err = ctm.removeTask(bucketName, task.Id)
				if err != nil {
					continue
				}
			}

		} else {
			log.Println("error happen!", errors.New("callback error"))
			continue //retry
		}

		return
	}
}

// getTask 获取第一个任务 -> 最接近执行时间的任务
func (ctm *CronTaskManager) getTask(bucketName string) (*Task, error) {
	value, err := ctm.redisClient.ZRangeFirst(bucketName)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	timestamp := int(value[0].(float64))
	taskId := value[1].(string)
	task := Task{
		Id:        taskId,
		Timestamp: timestamp,
	}
	return &task, nil
}

// getTaskById 获取任务 by taskId
func (ctm *CronTaskManager) getTaskById(task *Task) (*Task, error) {
	v, err := ctm.redisClient.Get(task.Id)
	if err != nil {
		return nil, err
	}
	if v == "" {
		return nil, nil
	}

	err = json.Unmarshal([]byte(v), &task)
	if err != nil {
		return nil, err
	}
	return task, nil
}

// removeTask 删除任务
func (ctm *CronTaskManager) removeTask(bucketName string, taskId string) error {
	err := ctm.redisClient.ZRem(bucketName, taskId)
	if err != nil {
		return err
	}
	err = ctm.redisClient.Del(taskId)
	if err != nil {
		return err
	}
	return nil
}
