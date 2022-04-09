package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	InputFiles []string

	MapTasks    []Task
	ReduceTasks []Task

	NumMapFinished    int
	NumReduceFinished int
	NMap              int
	NReduce           int

	Intermediates [][]string

	MapFinished    bool
	ReduceFinished bool

	mutex sync.Mutex
}

type Task struct {
	TaskType      string    // "Map", "Reduce", "Wait"
	TaskStatus    string    // "idle", "in-process", "completed"
	Timestamp     time.Time // start time
	TaskID        int
	NReduce       int
	MapFile       string
	Intermediates []KeyValue
}

func (m *Master) WorkerCallHandler(args *Args, reply *Reply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch args.MessageType {
	case "request":
		if !m.MapFinished {
			for index, task := range m.MapTasks {
				if task.TaskStatus == "idle" {
					m.MapTasks[index].TaskStatus = "in-process"
					reply.Task = m.MapTasks[index]
					go m.checkTimeout("Map", index, 10)
					return nil
				}
			}
			reply.Task.TaskType = "Wait"
			return nil
		} else if !m.ReduceFinished {
			for index, task := range m.ReduceTasks {
				if task.TaskStatus == "idle" {
					m.ReduceTasks[index].TaskStatus = "in-process"
					reply.Task = m.ReduceTasks[index]
					go m.checkTimeout("Reduce", index, 10)
					return nil
				}
			}
			reply.Task.TaskType = "Wait"
			return nil
		} else {
			return nil
		}
	case "completed":
		if args.Task.TaskType == "Map" {
			m.MapTasks[args.Task.TaskID].TaskStatus = "completed"
			m.NumMapFinished++
			if m.NumMapFinished == m.NMap {
				m.MapFinished = true
			}
		} else {
			m.ReduceTasks[args.Task.TaskID].TaskStatus = "completed"
			m.NumReduceFinished++
			if m.NumReduceFinished == m.NReduce {
				m.ReduceFinished = true
			}
		}
		return nil
	}

	return nil
}

func (m *Master) SendFilename(interFile *InterFile, reply *Reply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Intermediates[interFile.ReduceID] = append(m.Intermediates[interFile.ReduceID], interFile.Filename)
	return nil
}
func (m *Master) GetFilename(args *Args, replyFile *ReplyFile) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	replyFile.Files =  m.Intermediates[args.Task.TaskID]
	return nil
}

func (m *Master) checkTimeout(taskType string, num int, timeout int) {
	time.Sleep(time.Second * time.Duration(timeout))
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch taskType {
	case "Map":
		if m.MapTasks[num].TaskStatus == "in-process" {
			m.MapTasks[num].TaskStatus = "idle"
		}
	case "Reduce":
		if m.ReduceTasks[num].TaskStatus == "in-process" {
			m.ReduceTasks[num].TaskStatus = "idle"
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.ReduceFinished
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		InputFiles:     files,
		NReduce:        nReduce,
		MapFinished:    false,
		ReduceFinished: false,
		NMap:           len(files),
		Intermediates: make([][]string, nReduce),
		NumMapFinished: 0,
		NumReduceFinished: 0,
	}

	//create Map tasks
	for index, file := range files {
		tempTask := Task{
			TaskType:   "Map",
			TaskID:     index,
			TaskStatus: "idle",
			MapFile:    file,
			NReduce: nReduce,
		}
		m.MapTasks = append(m.MapTasks, tempTask)
	}
	//create Reduce tasks
	for i := 0; i < m.NReduce; i++ {
		tempTask := Task{
			TaskType:   "Reduce",
			TaskID:     i,
			TaskStatus: "idle",
			NReduce: nReduce,
		}
		m.ReduceTasks = append(m.ReduceTasks, tempTask)
	}

	m.server()
	return &m
}
