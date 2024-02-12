package main

import (
	"context"
	"flag"
	"golanglearning/new_project/cron-task-demo/pkg/redis"
	scheulder "golanglearning/new_project/cron-task-demo/pkg/scheduler"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"log"
	"os"
	"path/filepath"
	"time"
)

// 获取当前用户的主目录
func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE")
}

type TaskData struct {
	Name  string
	Image string
}

func main() {

	// 解析 kubeconfig 文件路径
	kubeconfig := flag.String("kubeconfig", filepath.Join(
		homeDir(), ".kube", "config"), "Path to kubeconfig file")

	// 建立与 Kubernetes API 的连接
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}

	// 创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	client := redis.NewClientWithDefaultOption("localhost:6379", "")
	ctm, err := scheulder.NewCronTaskManager(2*time.Second, "k8s-job-manager", func(data interface{}) bool {
		value, ok := data.(map[string]interface{})
		if ok {
			// 创建 Job 对象
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: value["Name"].(string),
				},
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{{
								Name:    value["Name"].(string) + "-container",
								Command: []string{"sh", "-c", "sleep 10s"},
								Image:   value["Image"].(string),
								Ports: []corev1.ContainerPort{{
									ContainerPort: 80,
								}},
							}},
						},
					},
				},
			}
			_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
			if err != nil {
				klog.Error("Create job error: ", err)
			}
			if err != nil && errors.IsAlreadyExists(err) {
				// 先删除原本的job
				klog.Error("Error maybe is AlreadyExists error: ", err)
				foreground := metav1.DeletePropagationForeground
				deleteOptions := metav1.DeleteOptions{PropagationPolicy: &foreground}
				if err = clientset.BatchV1().Jobs("default").Delete(context.Background(), value["Name"].(string), deleteOptions); err != nil {
					klog.Error("Delete job error: ", err)
					return false
				}
				// NOTE: 需要等待删除才执行create
				time.Sleep(time.Second * 60)
				_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
				if err != nil {
					klog.Error("Create job error: ", err)
					return false
				}

				return true
			}
		} else {
			log.Println("err...")
		}
		return true

	}, client)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("start ticker...")
	ctm.Start()

	task1 := scheulder.Task{Id: "1", Type: scheulder.OnceType, Data: &TaskData{Image: "busybox:1.28", Name: "task1"}, Delay: 10 * time.Second}
	task2 := scheulder.Task{Id: "2", Type: scheulder.OnceType, Data: &TaskData{Image: "busybox:1.28", Name: "task2"}, Delay: 50 * time.Second}
	task3 := scheulder.Task{Id: "3", Type: scheulder.OnceType, Data: &TaskData{Image: "busybox:1.28", Name: "task3"}, Delay: 80 * time.Second}
	task4 := scheulder.Task{Id: "4", Type: scheulder.CronType, Data: &TaskData{Image: "busybox:1.28", Name: "task4"}, Delay: 90 * time.Second}
	task5 := scheulder.Task{Id: "5", Type: scheulder.CronType, Data: &TaskData{Image: "busybox:1.28", Name: "task5"}, Delay: 50 * time.Second}
	ctm.AddTask(&task1)
	ctm.AddTask(&task2)
	ctm.AddTask(&task3)
	ctm.AddTask(&task4)
	ctm.AddTask(&task5)

	select {
	case <-time.After(time.Hour):
		ctm.Stop()
	}
}
