package service

import (
	"fmt"
	"log"
	"time"

	"github.com/numericals/queueSys/broker"
	"github.com/numericals/queueSys/storage"
)

type SnapshotManager struct {
	LastSnapshotTime time.Time
	WAL              *storage.WAL
	Broker           *broker.Broker
	SnapshotNotify   chan struct{}
}

func NewSnapshotManager(WAL *storage.WAL, Broker *broker.Broker) *SnapshotManager {
	return &SnapshotManager{
		WAL:            WAL,
		Broker:         Broker,
		SnapshotNotify: Broker.SnapshotNotify,
	}
}

func (s *SnapshotManager) Start() {
	defer s.Broker.Wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.SnapshotNotify:
			s.Broker.Mu.Lock()
			fmt.Println(s.Broker.EventsSinceLastSnapshot)
			if s.Broker.EventsSinceLastSnapshot >= 1000 {
				err := s.WAL.CreateSnapshot(s.Broker.Messages, s.Broker.DeadLetterQueue, s.Broker.LastAppliedEventID)
				if err != nil {
					log.Println("err while creating snapshot", err)
					continue
				}
				s.Broker.EventsSinceLastSnapshot = 0
				s.WAL.CleanUp(s.Broker.LastAppliedEventID)
				ticker.Reset(5 * time.Minute)
			}
			s.Broker.Mu.Unlock()
		case <-ticker.C:
			s.Broker.Mu.Lock()
			if s.Broker.EventsSinceLastSnapshot > 0 {
				err := s.WAL.CreateSnapshot(s.Broker.Messages, s.Broker.DeadLetterQueue, s.Broker.LastAppliedEventID)
				if err != nil {
					log.Println("err while creating snapshot", err)
					continue
				}
				s.Broker.EventsSinceLastSnapshot = 0
				s.WAL.CleanUp(s.Broker.LastAppliedEventID)
			}
			s.Broker.Mu.Unlock()
		case <-s.Broker.Ctx.Done():
			return
		}
	}
}

func (s *SnapshotManager) Flush() error {
	s.Broker.Mu.Lock()
	defer s.Broker.Mu.Unlock()
	if err := s.WAL.CreateSnapshot(
		s.Broker.Messages,
		s.Broker.DeadLetterQueue,
		s.Broker.LastAppliedEventID,
	); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	s.Broker.EventsSinceLastSnapshot = 0
	s.WAL.CleanUp(s.Broker.LastAppliedEventID)

	return nil
}
