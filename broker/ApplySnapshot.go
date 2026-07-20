package broker

import "github.com/numericals/queueSys/storage"

func (b *Broker) ApplySnapshot(snapshot *storage.Snapshot) {
	b.LastAppliedEventID = snapshot.Metadata.LastAppliedEventID
	if len(snapshot.Messages) > 0 {
		messages := storage.ConvertSnapshotMessagesToMessages(snapshot.Messages)
		b.Messages = messages
	}

	if len(snapshot.DeadLetterQueue) > 0 {
		message := storage.ConvertSnapshotMessagesToMessages(snapshot.DeadLetterQueue)
		b.DeadLetterQueue = message
	}
}
