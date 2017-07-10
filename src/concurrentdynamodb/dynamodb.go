package concurrentdynamodb

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	DynamodbInst               *ConcurrentDynamoDB
)

type ConcurrentDynamoDB struct {
	*dynamodb.DynamoDB
}

type dynamodbInfo struct {
	AccessKey string
	SecretKey string
	Region    string
}

func Connect(info dynamodbInfo) *ConcurrentDynamoDB {
	creds := credentials.NewStaticCredentials(info.AccessKey, info.SecretKey, "")
	conf := aws.Config{
		Credentials: creds,
		Region:      &info.Region,
	}
	dynamodbInst := &ConcurrentDynamoDB{dynamodb.New(session.New(), &conf)}
	return dynamodbInst

}

func (c *ConcurrentDynamoDB) mergeConcurrentResp(ddbResultChan chan interface{}, ddbErrChan chan error, doneChan chan bool) (chan []interface{}, chan error) {

	var mergedDDBResult []interface{}
	mergeResultChan := make(chan []interface{})
	mergeErrChan := make(chan error)

	go func() {
		defer close(mergeResultChan)
		defer close(mergeErrChan)

	MergeLoop:
		for {
			select {
			case ddbErr := <-ddbErrChan:
				mergeErrChan <- ddbErr
			case result := <-ddbResultChan:
				mergedDDBResult = append(mergedDDBResult, result)
			case <-doneChan:
				break MergeLoop
			}
		}
		mergeResultChan <- mergedDDBResult
	}()

	return mergeResultChan, mergeErrChan
}

func (c *ConcurrentDynamoDB) garbageClean(garbageResultChan chan []interface{}, garbageErrChan chan error) {
	// Create goroutine to consume garbage channel
GarbageClean:
	for {
		select {
		case restOfError := <-garbageErrChan:
			// Ignore nil error which caused by channel closed
			// Just print it for logging
			if restOfError != nil {
				fmt.Printf("Consume the rest of garbage error: %v", restOfError)
			}
		case <-garbageResultChan:
			break GarbageClean
		}
	}
}

func (c *ConcurrentDynamoDB) ConcurrentGetItem(input []*dynamodb.GetItemInput) (output []*dynamodb.GetItemOutput, err error) {
	ctx, cancel := context.WithCancel(context.Background())

	ddbResultChan := make(chan interface{})
	ddbErrChan := make(chan error)
	taskDoneChan := make(chan bool)

	go func() {
		var wg sync.WaitGroup

		for _, data := range input {
			wg.Add(1)
			go func(inputData *dynamodb.GetItemInput) {
				out, err := c.GetItem(inputData)

				if err != nil {
					ddbErrChan <- err
					cancel()
				} else {
					ddbResultChan <- out
				}
				wg.Done()
			}(data)
		}

		wg.Wait()
		taskDoneChan <- true
		close(taskDoneChan)
		close(ddbResultChan)
		close(ddbErrChan)
	}()

	mergeResultChan, mergeErrChan := DDB.mergeConcurrentResp(ddbResultChan, ddbErrChan, taskDoneChan)

	select {
	case mergeResult := <-mergeResultChan:
		for _, result := range mergeResult {
			output = append(output, result.(*dynamodb.GetItemOutput))
		}
	case <-ctx.Done():
		// Catch first error and return it to caller
		err = <-mergeErrChan

		go DynamodbInst.garbageClean(mergeResultChan, mergeErrChan)
	}

	return output, err
}

func (c *ConcurrentDynamoDB) ConcurrentBatchGetItem(input []*dynamodb.BatchGetItemInput) (output []*dynamodb.BatchGetItemOutput, err error) {
	ctx, cancel := context.WithCancel(context.Background())

	ddbResultChan := make(chan interface{})
	ddbErrChan := make(chan error)
	taskDoneChan := make(chan bool)

	go func() {
		var wg sync.WaitGroup

		for _, data := range input {
			wg.Add(1)
			go func(inputData *dynamodb.BatchGetItemInput) {
				out, err := c.BatchGetItem(inputData)

				if err != nil {
					ddbErrChan <- err
					cancel()
				} else {
					ddbResultChan <- out
				}
				wg.Done()
			}(data)
		}

		wg.Wait()
		taskDoneChan <- true
		close(taskDoneChan)
		close(ddbResultChan)
		close(ddbErrChan)
	}()

	mergeResultChan, mergeErrChan := DynamodbInst.mergeConcurrentResp(ddbResultChan, ddbErrChan, taskDoneChan)

	select {
	case mergeResult := <-mergeResultChan:
		for _, result := range mergeResult {
			output = append(output, result.(*dynamodb.BatchGetItemOutput))
		}
	case <-ctx.Done():
		// Catch first error and return it to caller
		err = <-mergeErrChan

		go DynamodbInst.garbageClean(mergeResultChan, mergeErrChan)
	}

	return output, err
}

