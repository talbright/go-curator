package plugin

import (
	// log "github.com/Sirupsen/logrus"
	// _ "github.com/assistly/birdhouse/log"
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
	work        *Work
	workers     *WorkerList
	workersLock *sync.Mutex
	nextWorker  int
	client      *Client
}

func NewWorkSupervisor(client *Client, workPath string) *WorkSupervisor {
	return &WorkSupervisor{
		client:      client,
		work:        NewWork(client, workPath),
		workers:     NewWorkerList(),
		workersLock: &sync.Mutex{},
	}
}

func (s *WorkSupervisor) AddWorker(node *Znode) (err error) {
	// log.WithField("node", node).Debug("add worker")
	w := NewWorker(s.client, node.Path)
	if !s.workers.Add(w) {
		// log.WithField("worker", w).WithField("snapshot", s.workers.ToSpew(3)).Warn("worker already exists")
		return ErrWorkerAlreadyExists
	}
	w.Children.LoadCache()
	w.Children.Clear()
	totalWork := s.work.Children.Size()
	workerCount := s.workers.Size()
	nodesToAdd := make([]Znode, 0)
	// log.WithField("totalWork", totalWork).WithField("workerCount", workerCount).Debug("preliminary calculations for work distribution")
	if workerCount > 1 && totalWork > 0 {
		workPerWorker := int(totalWork / workerCount)
		workRemainder := totalWork % workerCount
		if workRemainder != 0 {
			workPerWorker = workPerWorker - int(workRemainder/(workerCount-1))
		}
		// log.WithField("workPerWorker", workPerWorker).WithField("remainder", workRemainder).Debug("post calculations for work distribution")
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
				// log.WithField("amount", shiftAmount).WithField("sliced", sliced).WithField("worker", worker).Debug("work removed from existing worker")
				nodesToAdd = append(nodesToAdd, sliced...)
				if len(nodesToAdd) > workPerWorker {
					break
				}
			}
		}
	} else {
		nodesToAdd = s.work.Children.ToSlice()
	}
	// log.WithField("nodes", nodesToAdd).WithField("worker", w).Debug("preparing to add nodes to worker")
	w.UnshiftWork(nodesToAdd)
	return nil
}

func (s *WorkSupervisor) RemoveWorker(node *Znode) (err error) {
	// log.WithField("node", node).Debug("removing worker")
	_, worker := s.workers.FindById(node.Path)
	if worker == nil {
		return ErrWorkerNotFound
	}
	children := worker.Children.ToSlice()
	if err = worker.Children.Clear(); err != nil {
		// log.WithError(err).WithField("worker", worker).Warn("unable to clear cache")
	}
	if !s.workers.Remove(worker) {
		return ErrUnableToRemoveWorker
	}
	for _, node := range children {
		newNode := node
		if assignErr := s.AssignWork(&newNode); assignErr != nil {
			// log.WithError(assignErr).Warn("unable to assign work to worker")
		}
	}
	return err
}

func (s *WorkSupervisor) AddWork(node *Znode) (err error) {
	// log.WithField("node", node).Debug("add work")
	if err = s.work.Children.Add(node); err == nil {
		if err = s.AssignWork(node); err != nil {
			// log.WithError(err).WithField("node", node).Warn("unable to assign work to worker")
		}
	}
	return err
}

func (s *WorkSupervisor) RemoveWork(node *Znode) (err error) {
	// log.WithField("node", node).Debug("remove work")
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
	// log.Debug("loading cache")
	s.work.Children.LoadCache()
	for _, w := range s.workers.ToSlice() {
		w.Children.LoadCache()
		w.Children.Clear()
	}
	for _, n := range s.work.Children.ToSlice() {
		s.AddWork(&n)
	}
	return nil
}

func (s *WorkSupervisor) AssignWork(work *Znode) error {
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
	// log.WithField("worker", worker).WithField("work", work).WithField("workerCount", workerCount).WithField("index", s.nextWorker).Debug("assigning work")
	s.nextWorker++
	err := worker.Children.Add(work)
	// log.WithField("worker", fmt.Sprintf("%#v", worker)).WithField("children", worker.Children.ToSlice()).Debug("post addition")
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
