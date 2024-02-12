package scheulder

import (
	"golanglearning/new_project/cron-task-demo/pkg/redis"
	"log"
	"testing"
	"time"
)

func TestRedisDelay(t *testing.T) {
	client := redis.NewClientWithDefaultOption("localhost:6379", "")
	ctm, err := NewCronTaskManager(2*time.Second, "test", func(data interface{}) bool {
		log.Println("do task ", data)
		return true
	}, client)
	if err != nil {
		t.Error(err)
	}
	log.Println("start ticker...")
	ctm.Start()

	task1 := Task{Id: "1", Type: CronType, Data: "task1", Delay: 5 * time.Second}
	task2 := Task{Id: "2", Type: OnceType, Data: "task2", Delay: 8 * time.Second}
	task3 := Task{Id: "3", Type: OnceType, Data: "task3", Delay: 5 * time.Second}
	task4 := Task{Id: "4", Type: CronType, Data: "task4", Delay: 5 * time.Second}
	task5 := Task{Id: "5", Type: CronType, Data: "task5", Delay: 5 * time.Second}
	ctm.AddTask(&task1)
	ctm.AddTask(&task2)
	ctm.AddTask(&task3)
	ctm.AddTask(&task4)
	ctm.AddTask(&task5)

	select {
	case <-time.After(time.Minute):
		ctm.Stop()
	}
}
