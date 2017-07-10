package concurrentdynamodb

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
	benchTestCount = int64(1000)
	testDynamodbTableName = "concurrentTesting"
)

func init() {
	// You should provide your dynamodb information to run this test
	DynamodbInst = Connect(
		dynamodbInfo{
			Region:    "Your dynamodb region",
			AccessKey: "Your dynamodb access key",
			SecretKey: "Your dynamodb secret key",
		},
	)
}

func Test_ConcurrentGetItem(t *testing.T) {
	testStructs := PrepareTestData(DynamodbInst, 10)
	testData := PrepareGetItemInput(testStructs)

	result, err := DynamodbInst.ConcurrentGetItem(testData)
	if err != nil {
		t.Errorf("Error: %v", err)
	} else {
		t.Logf("Return Item: %d", len(result))
		if len(result) != len(testStructs) {
			t.Errorf("Ask %d items, but only return %d items", len(testStructs), len(result))
		}
	}
}

func Benchmark_ConcurrentGetItem(b *testing.B) {
	testStructs := PrepareTestData(DynamodbInst, benchTestCount)
	testData := PrepareGetItemInput(testStructs)

	b.ResetTimer()
	for count := 0; count < b.N; count++ {
		result, err := DynamodbInst.ConcurrentGetItem(testData)
		if err != nil {
			b.Errorf("Error: %v", err)
		} else {
			if len(result) != len(testData) {
				b.Errorf("Ask %d item(s), but only return %d item(s)", len(testData), len(result))
			}
		}
	}
}

func Benchmark_ConcurrentBatchGetItem(b *testing.B) {
	testStructs := PrepareTestData(DynamodbInst, benchTestCount)
	testData := PrepareBatchGetItemInput(testStructs)

	b.ResetTimer()
	for count := 0; count < b.N; count++ {
		result, err := DynamodbInst.ConcurrentBatchGetItem(testData)
		if err != nil {
			b.Errorf("Error: %v", err)
		} else {
			if len(result) != len(testData) {
				b.Errorf("Ask %d item(s), but only return %d item(s)", len(testData), len(result))
			}
		}
	}
}
