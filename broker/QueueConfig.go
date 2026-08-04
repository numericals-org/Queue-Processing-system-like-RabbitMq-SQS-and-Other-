package broker

import (
	"fmt"

	"github.com/numericals/queueSys/types"
)

func ValidateConfig(config types.QueueConfig) error {
	if config.DefaultRetryDelay < 0 {
		return fmt.Errorf("DefaultRetryDelay cannot be negative")
	}

	if config.MaxAllowedDelay < 0 {
		return fmt.Errorf("MaxAllowedDelay cannot be negative")
	}

	if config.MaxDeliveryAttempt < 1 {
		return fmt.Errorf("MaxDeliveryAttempt must be at least 1")
	}

	if config.VisibilityTimeout < 0 {
		return fmt.Errorf("VisibilityTimeout cannot be negative")
	}

	if config.MaxMessages < 0 {
		return fmt.Errorf("MaxMessages cannot be negative")
	}

	return nil
}
