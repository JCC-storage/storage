package cmdline

import (
	"fmt"
	"os"
	"strconv"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/client/internal/services"
)

type Commandline struct {
	svc *services.Service
}

func NewCommandline(svc *services.Service) (*Commandline, error) {
	return &Commandline{
		svc: svc,
	}, nil
}

func (c *Commandline) DispatchCommand(cmd string, args []string) {
	switch cmd {
	case "read":
		objectID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}

		if err := c.Read(args[0], objectID); err != nil {
			fmt.Printf("read failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "write":
		bucketID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}
		numRep, _ := strconv.Atoi(args[3])
		if numRep <= 0 || numRep > config.Cfg().MaxReplicateNumber {
			fmt.Printf("replicate number should not be more than %d", config.Cfg().MaxReplicateNumber)
			os.Exit(1)
		}

		if err := c.RepWrite(args[0], bucketID, args[2], numRep); err != nil {
			fmt.Printf("rep write failed, err: %s", err.Error())
			os.Exit(1)
		}
	case "ecWrite":
		bucketID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}
		if err := c.EcWrite(args[0], bucketID, args[2], args[3]); err != nil {
			fmt.Printf("ec write failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "move":
		objectID, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Printf("invalid object id %s, err: %s", args[0], err.Error())
			os.Exit(1)
		}
		stgID, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Printf("invalid storage id %s, err: %s", args[1], err.Error())
			os.Exit(1)
		}

		if err := c.Move(objectID, stgID); err != nil {
			fmt.Printf("move failed, err: %s", err.Error())
			os.Exit(1)
		}

	case "ls":
		if len(args) == 0 {
			if err := c.GetUserBuckets(); err != nil {
				fmt.Printf("get user buckets failed, err: %s", err.Error())
				os.Exit(1)
			}
		} else {
			bucketID, err := strconv.Atoi(args[0])
			if err != nil {
				fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.GetBucketObjects(bucketID); err != nil {
				fmt.Printf("get bucket objects failed, err: %s", err.Error())
				os.Exit(1)
			}
		}
	case "bucket":
		if len(args) == 0 {
			fmt.Printf("need more arg")
			os.Exit(1)
		}
		cmd := args[0]

		if cmd == "new" {
			if err := c.CreateBucket(args[1]); err != nil {
				fmt.Printf("create bucket failed, err: %s", err.Error())
				os.Exit(1)
			}
		} else if cmd == "delete" {
			bucketID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid bucket id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.DeleteBucket(bucketID); err != nil {
				fmt.Printf("delete bucket failed, err: %s", err.Error())
				os.Exit(1)
			}
		}

	case "object":
		if len(args) == 0 {
			fmt.Printf("need more arg")
			os.Exit(1)
		}

		if args[0] == "delete" {
			objectID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("invalid object id %s, err: %s", args[1], err.Error())
				os.Exit(1)
			}

			if err := c.DeleteObject(objectID); err != nil {
				fmt.Printf("delete object failed, err: %s", err.Error())
				os.Exit(1)
			}
		}
	}

}
