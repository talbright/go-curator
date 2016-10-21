package plugin

import (
	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"

	"errors"
	"sync"
)

var ErrNoAvailableWorkers = errors.New("no workers available to process work")
var ErrWorkerNotFound = errors.New("worker not found")
var ErrWorkNotFound = errors.New("work not found")
var ErrWorkerAlreadyExists = errors.New("worker already exists")
var ErrUnableToRemoveWorker = errors.New("could not remove worker")

type WorkSupervisorType interface {
	AddWorker(worker *Worker) error
	RemoveWorker(worker *Worker) error
	AddWork(node *Znode) error
	RemoveWork(node *Znode) error
	Load() error
}

type WorkSupervisor struct {
	work         *Work
	workers      *WorkerList
	workersLock  *sync.Mutex
	nextWorker   int
	client       *Client
	Logger       *log.Logger
	LogComponent string
}

func NewWorkSupervisor(client *Client, workPath string) *WorkSupervisor {
	return &WorkSupervisor{
		client:       client,
		work:         NewWork(client, workPath),
		workers:      NewWorkerList(),
		workersLock:  &sync.Mutex{},
		Logger:       log.StandardLogger(),
		LogComponent: "work_supervisor",
	}
}

func (s *WorkSupervisor) entry() *log.Entry {
	return s.Logger.WithField("component", s.LogComponent)
}

func (s *WorkSupervisor) AddWorker(node *Znode) (err error) {
	entry := s.entry().WithField("node", spew.Sprintf("%#v", node)).WithField("method", "AddWorker")
	entry.Info("adding worker")
	w := NewWorker(s.client, node.Path)
	if !s.workers.Add(w) {
		return ErrWorkerAlreadyExists
	}
	w.Children.LoadCache()
	w.Children.Clear()
	totalWork := s.work.Children.Size()
	workerCount := s.workers.Size()
	nodesToAdd := make([]Znode, 0)
	entry.WithFields(log.Fields{
		"totalWork":   totalWork,
		"workerCount": workerCount,
	}).Debug("preliminary calculations for work distribution")
	if workerCount > 1 && totalWork > 0 {
		workPerWorker := int(totalWork / workerCount)
		workRemainder := totalWork % workerCount
		if workRemainder != 0 {
			workPerWorker = workPerWorker - int(workRemainder/(workerCount-1))
		}
		entry.WithFields(log.Fields{
			"workerPerWorker": workPerWorker,
			"remainder":       workRemainder,
		}).Debug("post calculations for work distribution")
		if workPerWorker > 0 {
			for _, worker := range s.workers.ToSlice() {
				if worker.Id() == w.Id() {
					continue
				}
				var shiftAmount int
				if workRemainder == 0 {
					shiftAmount = worker.Children.Size() - workPerWorker
				} else {
					shiftAmount = workPerWorker
				}
				capacityLeft := worker.Children.Size() - shiftAmount
				if capacityLeft <= 0 {
					continue
				}
				if capacityLeft < workPerWorker {
					shiftAmount = worker.Children.Size() % workPerWorker
				}
				sliced := worker.ShiftWork(shiftAmount)
				entry.WithFields(log.Fields{
					"sliced": sliced,
					"worker": worker,
				}).Debug("work removed from existing worker")
				nodesToAdd = append(nodesToAdd, sliced...)
				if len(nodesToAdd) > workPerWorker {
					break
				}
			}
		}
	} else {
		nodesToAdd = s.work.Children.ToSlice()
	}
	entry.WithFields(log.Fields{
		"appended": nodesToAdd,
		"worker":   w,
	}).Debug("preparing to add nodes to worker")
	w.UnshiftWork(nodesToAdd)
	return nil
}

func (s *WorkSupervisor) RemoveWorker(node *Znode) (err error) {
	entry := s.entry().WithField("node", spew.Sprintf("%#v", node)).WithField("method", "RemoveWorker")
	entry.Info("removing worker")
	_, worker := s.workers.FindById(node.Path)
	if worker == nil {
		return ErrWorkerNotFound
	}
	children := worker.Children.ToSlice()
	if err = worker.Children.Clear(); err != nil {
		entry.WithError(err).WithField("worker", worker).Warn("unable to clear cache")
	}
	if !s.workers.Remove(worker) {
		return ErrUnableToRemoveWorker
	}
	for _, node := range children {
		newNode := node
		if assignErr := s.AssignWork(&newNode); assignErr != nil {
			entry.WithError(assignErr).WithField("node", node).WithField("worker", worker).Warn("unable to assign work to worker")
		}
	}
	return err
}

func (s *WorkSupervisor) AddWork(node *Znode) (err error) {
	entry := s.entry().WithField("node", spew.Sprintf("%#v", node)).WithField("method", "AddWork")
	entry.Info("add work")
	if err = s.work.Children.Add(node); err == nil {
		if err = s.AssignWork(node); err != nil {
			entry.WithError(err).WithField("node", node).Warn("unable to assign work to worker")
		}
	}
	return err
}

func (s *WorkSupervisor) RemoveWork(node *Znode) (err error) {
	entry := s.entry().WithField("node", spew.Sprintf("%#v", node)).WithField("method", "RemoveWork")
	entry.Info("remove work")
	if err = s.work.Children.Remove(node); err == nil {
		_, worker := s.workers.Find(func(i int, w *Worker) (found bool) {
			if w.Children.Contains(node.Name) {
				err = w.Children.Remove(node)
				found = true
			}
			return found
		})
		if err != nil {
			return err
		}
		if worker == nil {
			return ErrNoAvailableWorkers
		}
	}
	if err == ErrNodeNotInCache {
		err = ErrWorkNotFound
	}
	return err
}

func (s *WorkSupervisor) Load() (err error) {
	entry := s.entry().WithField("method", "Load")
	entry.Info("loading cache")
	if err := s.work.Children.LoadCache(); err != nil {
		entry.WithError(err).Error("error loading work cache")
	}
	for _, w := range s.workers.ToSlice() {
		if err := w.Children.LoadCache(); err != nil {
			entry.WithField("worker", w).WithError(err).Error("error loading worker cache")
		}
		w.Children.Clear()
	}
	for _, n := range s.work.Children.ToSlice() {
		s.AddWork(&n)
	}
	return nil
}

func (s *WorkSupervisor) AssignWork(work *Znode) error {
	entry := s.entry().WithField("work", work).WithField("method", "AssignWork")
	entry.Info("assigning work")
	s.workersLock.Lock()
	defer s.workersLock.Unlock()
	workerCount := s.workers.Size()
	if workerCount < 1 {
		s.nextWorker = 0
		return ErrNoAvailableWorkers
	}
	if s.nextWorker >= workerCount {
		s.nextWorker = 0
	}
	worker := s.workers.At(s.nextWorker)
	if worker == nil {
		return ErrNoAvailableWorkers
	}
	s.nextWorker++
	err := worker.Children.Add(work)
	entry.WithFields(log.Fields{
		"worker":      worker,
		"workerCount": workerCount,
		"index":       s.nextWorker,
		"children":    worker.Children.ToSlice(),
		"error":       err,
	}).Debug("post assignment info")
	return err
}

func (s *WorkSupervisor) GetWorkers() *WorkerList {
	return s.workers
}

func (s *WorkSupervisor) GetWork() *Work {
	return s.work
}

func (s *WorkSupervisor) ToSpew(depth int) string {
	return s.work.ToSpew(depth) + "\n" + s.workers.ToSpew(depth)
}
