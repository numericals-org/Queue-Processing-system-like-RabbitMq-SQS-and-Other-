package storage

import "github.com/numericals/queueSys/types"

func ConvertToSnapshotMessage(message types.Message) SnapshotMessage {
	return SnapshotMessage{
		MessageId:        message.MessageId,
		Content:          message.Content,
		Progress:         message.Progress,
		DeliveryAttempts: message.DeliveryAttempts,
		LastConsumerId:   message.LastConsumerId,
		RetryAfter:       message.RetryAfter,
		RetrieveAt:       message.RetrieveAt,
	}
}

func ConvertToMessage(message SnapshotMessage) types.Message {
	return types.Message{
		MessageId:        message.MessageId,
		Content:          message.Content,
		Progress:         message.Progress,
		DeliveryAttempts: message.DeliveryAttempts,
		LastConsumerId:   message.LastConsumerId,
		RetryAfter:       message.RetryAfter,
		RetrieveAt:       message.RetrieveAt,
	}
}

func ConvertMessagesToSnapshotMessages(messages []types.Message) []SnapshotMessage {
	var SnapshotMessages []SnapshotMessage

	for i := range messages {
		MSG := ConvertToSnapshotMessage(messages[i])
		SnapshotMessages = append(SnapshotMessages, MSG)
	}

	return SnapshotMessages
}

func ConvertSnapshotMessagesToMessages(snapshots []SnapshotMessage) []types.Message {
	var Messages []types.Message

	for i := range snapshots {
		MSG := ConvertToMessage(snapshots[i])
		Messages = append(Messages, MSG)
	}

	return Messages
}
