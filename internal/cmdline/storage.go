package cmdline

import (
	"fmt"
	"time"
)

func StorageMoveObjectToStorage(ctx CommandContext, objectID int, storageID int) error {
	taskID, err := ctx.Cmdline.Svc.StorageSvc().StartMovingObjectToStorage(0, objectID, storageID)
	if err != nil {
		return fmt.Errorf("start moving object to storage: %w", err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.StorageSvc().WaitMovingObjectToStorage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("moving complete with: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait moving: %w", err)
		}
	}
}

func init() {
	commands.MustAdd(StorageMoveObjectToStorage, "storage", "move")
}
