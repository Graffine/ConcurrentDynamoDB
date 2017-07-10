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

type ConcurrentTesting struct {
	Tid int `json:"tid"`
	Rid int `json:"Rid"`
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

	mergeResultChan, mergeErrChan := DynamodbInst.mergeConcurrentResp(ddbResultChan, ddbErrChan, taskDoneChan)

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

// TODO: Make parameter/resp generics
func PrepareTestData(c *ConcurrentTesting, limit int64) (testData []ConcurrentTesting) {
	param := &dynamodb.ScanInput{
		TableName:            aws.String("concurrentTesting"),
		ProjectionExpression: aws.String("tid, rid"),
		Limit:                aws.Int64(limit),
	}

	resp, _ := c.Scan(param)
	for _, item := range resp.Items {
		data := ConcurrentTesting{}
		dynamodbattribute.ConvertFromMap(item, &data)
		testData = append(testData, data)
	}
	return
}

// TODO: Make parameter generics
func PrepareGetItemInput(testConcurrentTestings []ConcurrentTesting) (testData []*dynamodb.GetItemInput) {
	for _, item := range testConcurrentTestings {
		ddbData := &dynamodb.GetItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"tid": {
					N: aws.String(strconv.Itoa(item.Pid)),
				},
				"rid": {
					N: aws.String(strconv.Itoa(item.Cid)),
				},
			},
			TableName:              aws.String("concurrentTesting"),
			ReturnConsumedCapacity: aws.String("INDEXES"),
		}
		testData = append(testData, ddbData)
	}
	return
}

// TODO: Make parameter generics
func PrepareBatchGetItemInput(testConcurrentTestings []ConcurrentTesting) (testData []*dynamodb.BatchGetItemInput) {
	var chunkSize = 100

	var ddbAttributeValueData []map[string]*dynamodb.AttributeValue
	for _, item := range testConcurrentTestings {
		data := map[string]*dynamodb.AttributeValue{
			"tid": {
				N: aws.String(strconv.Itoa(item.Pid)),
			},
			"rid": {
				N: aws.String(strconv.Itoa(item.Cid)),
			},
		}
		ddbAttributeValueData = append(ddbAttributeValueData, data)
	}

	var chunkData [][]map[string]*dynamodb.AttributeValue
	for start := 0; start < len(ddbAttributeValueData); start = start + chunkSize {
		end := start + chunkSize
		if end > len(ddbAttributeValueData) {
			end = len(ddbAttributeValueData)
		}
		chunkData = append(chunkData, ddbAttributeValueData[start:end])
	}

	for _, chunk := range chunkData {
		ddbData := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				"concurrentTesting": {
					Keys: chunk,
				},
			},
			ReturnConsumedCapacity: aws.String("INDEXES"),
		}
		testData = append(testData, ddbData)
	}

	return
}

