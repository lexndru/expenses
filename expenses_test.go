// Copyright (c) 2021 Alexandru Catrina
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.s
package expenses

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	T_USER = os.Getenv("DB_USER")
	T_PASS = os.Getenv("DB_PASS")
	T_ADDR = os.Getenv("DB_ADDR")
	T_NAME = os.Getenv("DB_NAME")
)

var (
	_MySQL  = mysql.Open(T_USER + ":" + T_PASS + "@tcp(" + T_ADDR + ")/" + T_NAME + "?charset=utf8mb4&parseTime=True")
	_SQLite = sqlite.Open("file::memory:?cache=shared")
)

func begin(layer gorm.Dialector) (db *gorm.DB) {
	var err error

	if db, err = gorm.Open(layer, &gorm.Config{}); err != nil {
		panic(err)
	}

	Install(db)

	return db
}

func close(db *gorm.DB) {
	Uninstall(db)

	// TODO(lexndru): how to close DB?
}

func testActorsAPI(t *testing.T, db *gorm.DB) {
	a := NewActor("Alexandru")

	newActors := Actors{a}
	if err := newActors.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var fewActors Actors
	if err := fewActors.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(fewActors) != 1 || fewActors[0].Name != newActors[0].Name {
		t.Fatal("Pulled actor doesn't match with pushed actor")
	}

	moreActors := Actors{
		Actor{
			Name:    "XYZ",
			Flags:   2,
			Headers: "image=/path/to/img",
		},
		Actor{
			Name:    "Hypermarket",
			Headers: "image=/path/to/img",
		},
	}

	fewActors[0].Flags = 29 // push must upsert
	fewActors = append(fewActors, moreActors...)

	if err := fewActors.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	if err := fewActors.Pull(PullContext{Storage: db, Limit: 5}); err != nil {
		t.Fatal(err)
	}

	if len(fewActors) != 3 {
		t.Fatalf("Pushed 3 actors and got back %d instead\n", len(fewActors))
	}
}

func testLabelsAPI(t *testing.T, db *gorm.DB) {
	newLabels := Labels{
		Label{
			Name: "Tabletă",
		},
	}

	if err := newLabels.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var fewLabels Labels
	if err := fewLabels.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(fewLabels) != 1 || fewLabels[0].Name != newLabels[0].Name {
		t.Fatal("Pulled label doesn't match with pushed label")
	}

	moreLabels := Labels{
		Label{
			Name: "Alimente",
		},
		Label{
			Name:   "Pâine",
			Parent: &Label{Name: "Alimente"},
		},
	}

	fewLabels[0].Parent = &Label{
		Name:   "Electronice",
		Parent: &Label{Name: "Bunuri"},
	}

	fewLabels = append(fewLabels, moreLabels...)

	if err := fewLabels.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	if err := fewLabels.Pull(PullContext{Storage: db, Limit: 10}); err != nil {
		t.Fatal(err)
	}

	if len(fewLabels) != 5 {
		t.Fatalf("Pushed 5 labels and got back %d instead\n", len(fewLabels))
	}
}

func testTransactionsAPI(t *testing.T, db *gorm.DB) {
	date, _ := time.Parse("2006-01-02", "2021-04-22")
	trx := NewTransaction(date, -3000, NewLabel("Alimente", nil), NewActor("Alexandru"), NewActor("Magazin"), nil)

	newTransactions := Transactions{trx}
	if err := newTransactions.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var fewTransactions Transactions
	if err := fewTransactions.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(fewTransactions) != 1 || fewTransactions[0].UUID == nil || *fewTransactions[0].UUID == "" {
		t.Fatal("Pulled transaction doesn't match with pushed transaction")
	}

	recentTransactionWithUUID := fewTransactions[0] // this should not get duplicated, but updated
	recentTransactionWithUUID.Label.Name = "?"
	recentTransactionWithUUID.Flags = 29
	recentTransactionWithUUID.Details = []*Details{
		{
			LabelName: "Pâine",
			Amount:    1000,
		},
		{
			LabelName: "Prăjitură",
			Amount:    2000,
		},
	}

	moreTransactions := Transactions{
		Transaction{
			Date:         date.AddDate(0, 0, 1),
			Amount:       5000, // 50.00
			LabelName:    "Transfer",
			SenderName:   "?",
			ReceiverName: "Alexandru",
		},
		Transaction{
			Date:         date.AddDate(0, 1, 0),
			Amount:       -15000, // -150.00
			LabelName:    "?",
			SenderName:   "Alexandru Catrina",
			ReceiverName: "Magazin",
			Details: []*Details{
				{
					LabelName: "Alimente",
					Amount:    5000, // 50.00
				},
				{
					LabelName: "Hrană pentru animale",
					Amount:    8000, // 80.00
				},
				{
					LabelName: "Apă",
					Amount:    2000, // 20.00
				},
			},
		},
	}

	moreTransactions = append(moreTransactions, recentTransactionWithUUID) // 2 inserts and 1 update

	if err := moreTransactions.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	if err := moreTransactions.Pull(PullContext{Storage: db, Limit: 5}); err != nil {
		t.Fatal(err)
	}

	if len(moreTransactions) != 3 {
		t.Fatalf("Pushed 3 transactions and got back %d instead\n", len(fewTransactions))
	}

	numDetails := 0
	for _, trx := range moreTransactions {
		for _, ls := range trx.Details {
			if ls.Amount > 0 {
				numDetails++
			}
		}
	}

	if numDetails != 5 { // counted all by myself... must be correct :)
		t.Fatalf("Expected 5 details on 2 transactions but instead got %d", numDetails)
	}
}

func testTransactionJustAppendFlag(t *testing.T, db *gorm.DB) {
	date, _ := time.Parse("2006-01-02", "2021-04-27")

	generatedId := uuid.New().String()

	trxs := Transactions{
		Transaction{
			UUID:         &generatedId,
			Date:         date,
			Amount:       1,
			LabelName:    "?",
			SenderName:   "?",
			ReceiverName: "?",
		},
	}

	if err := trxs.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var transactionsCopy Transactions
	if err := transactionsCopy.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(transactionsCopy) != 1 {
		t.Fail()
	}

	if *transactionsCopy[0].UUID != generatedId {
		t.Fatal("Transaction UUID does not match with generated one")
	}

	var theSameTrx Transaction
	theSameTrx = Transaction{
		UUID:         &generatedId,
		Date:         date,
		Amount:       1,
		LabelName:    "new label for this transaction",
		SenderName:   "?",
		ReceiverName: "?",
	}

	if err := (&Transactions{theSameTrx}).Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var transactionsCopy2 Transactions
	if err := transactionsCopy2.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(transactionsCopy2) != 1 {
		t.Fail()
	}

	if transactionsCopy2[0].LabelName != "new label for this transaction" {
		t.Fatal("Expected transaction to have new label")
	}

	theSameTrx = Transaction{
		UUID:         &generatedId,
		Date:         date,
		Amount:       1,
		LabelName:    "?",
		SenderName:   "changed to another sender",
		ReceiverName: "?",
	}

	if err := (&Transactions{theSameTrx}).Push(PushContext{Storage: db, BatchSize: 10, JustAppend: true}); err != nil {
		t.Fatal(err)
	}

	var transactionsCopy3 Transactions
	if err := transactionsCopy3.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(transactionsCopy3) != 1 {
		t.Fail()
	}

	if transactionsCopy2[0].SenderName != "?" {
		t.Fatal("Expected transaction to have the original sender name")
	}
}

func testIncorrectAmountForTransaction(t *testing.T, db *gorm.DB) {
	date, _ := time.Parse("2006-01-02", "2021-04-23")

	lb := NewLabel("?", nil)
	ls := make(map[Label]int64, 1)
	ls[lb] = 50

	trx := NewTransaction(date, -100, NewLabel("?", nil), NewActor("?"), NewActor("?"), ls)
	incorrectTransactions := Transactions{trx}

	if err := incorrectTransactions.Push(PushContext{Storage: db, BatchSize: 10}); err == nil {
		t.Fatal("Expected to fail since amount doesn't add up, but instead got not error")
	}

	var transactionsCopy Transactions
	if err := transactionsCopy.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(transactionsCopy) != 0 {
		t.Fatalf("Expected 0 transactions after push because of incorrect sum but got %d instead\n", len(transactionsCopy))
	}

	incorrectTransactions[0].Details[0].Amount = 150
	incorrectTransactions[0].Details = append(incorrectTransactions[0].Details, &Details{
		Amount:    -50,
		LabelName: "??",
	})

	if err := incorrectTransactions.Push(PushContext{Storage: db, BatchSize: 10}); err == nil {
		t.Fatal("Should have failed because of negative details amount")
	}

	incorrectTransactions[0].Details = []*Details{{Amount: 100, LabelName: "???"}}

	if err := incorrectTransactions.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	if err := incorrectTransactions.Pull(PullContext{Storage: db, Limit: 2}); err != nil {
		t.Fatal(err)
	}

	if len(incorrectTransactions) != 1 {
		t.Fatal("Expected 1 transaction to be updated with correct values but instead it got duplicated")
	}
}

func testPrimaryRequestsWithTransactions(t *testing.T, db *gorm.DB) {
	date, _ := time.Parse("2006-01-02", "2021-04-25")

	trxs := Transactions{
		Transaction{
			Date:   date,
			Amount: -3000, // 30.00
			Label: &Label{
				Name: "?",
			},
			Sender:   &Actor{Name: "Alexandru Catrina"},
			Receiver: &Actor{Name: "Piață"},
			Details: []*Details{
				{
					Label:  &Label{Name: "Apă"},
					Amount: 1000, // 10.00
				},
				{
					Label: &Label{
						Name:   "Hrană pentru animale",
						Parent: &Label{Name: "Petshop"},
					},
					Amount: 2000, // 20.00
				},
			},
		},
		Transaction{
			Date:   date,
			Amount: 5000, // 50.00
			Label: &Label{
				Name: "Încasare",
				Parent: &Label{
					Name:   "Transfer intern",
					Parent: &Label{Name: "Intrări"},
				},
			},
			Sender:   &Actor{Name: "Alexandru Catrina"},
			Receiver: &Actor{Name: "Catrina Alexandru"},
		},
	}

	if _, err := NewPushRequest(&trxs, PushContext{Storage: db, BatchSize: 1}); err != nil {
		t.Fatal(err)
	} else {
		var trxProbe Transactions
		if _, err := NewPullRequest(&trxProbe, PullContext{Storage: db, Limit: 10}); err != nil {
			t.Fatal(err)
		} else {
			if len(trxs) != len(trxProbe) {
				t.Fatal("Expected same number of transactions")
			}

			for _, a := range trxs {
				var foundIt bool
				for _, b := range trxProbe {
					if a.Amount == b.Amount && a.Sender.Name == b.Sender.Name && a.Receiver.Name == b.Receiver.Name {
						foundIt = true
					}
				}

				if !foundIt {
					t.Fatal("Expected pull transaction to be found in pushed transaction")
				}
			}
		}
	}
}

func testPrimaryRequestsWithLabelsTree(t *testing.T, db *gorm.DB) {
	expectedJson := `[{"name":"9","parent":"8","flags":0,"headers":""},{"name":"10","parent":null,"flags":0,"headers":""}]`

	labels := Labels{
		Label{
			Name: "9",
			Parent: &Label{
				Name: "8",
				Parent: &Label{
					Name: "7",
					Parent: &Label{
						Name: "6",
						Parent: &Label{
							Name: "5",
							Parent: &Label{
								Name: "4",
								Parent: &Label{
									Name: "3",
									Parent: &Label{
										Name: "2",
										Parent: &Label{
											Name:   "1",
											Parent: &Label{Name: "0"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		Label{Name: "10"},
	}

	if out, err := NewPushRequest(&labels, PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	} else {
		if expectedJson != string(out) {
			t.Fatal("Expected output doesn't match with provided output")
		}
	}
}

func testLabelParentUpsert(t *testing.T, db *gorm.DB) {
	parent3 := NewLabel("Label #3", nil)
	parent1 := NewLabel("Label #1", nil)

	lb5 := NewLabel("Label #5", &parent3)
	lb2 := NewLabel("Label #2", &parent1)

	lbs := Labels{lb5, lb2}

	if err := lbs.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var lbCopy Labels
	if err := lbCopy.Pull(PullContext{Storage: db}); err != nil {
		t.Fatal(err)
	}

	if len(lbCopy) != 4 {
		t.Fatalf("Expected 4 labels but got %v instead\n", len(lbCopy))
	}

	for _, lb := range lbCopy {
		if lb.Name == "Label #5" && lb.Parent.Name != "Label #3" {
			t.Fatalf("Expected Label #5 to have parent Label #3 but instead has %v", lb.Parent)
		}
		if lb.Name == "Label #2" && lb.Parent.Name != "Label #1" {
			t.Fatalf("Expected Label #2 to have parent Label #1 but instead has %v", lb.Parent)
		}
	}

	dupLbs := Labels{
		Label{
			Name:   "Label #1",
			Parent: &lb5,
		},
	}

	if err := dupLbs.Push(PushContext{Storage: db, BatchSize: 10}); err != nil {
		t.Fatal(err)
	}

	var lbCopy2 Labels
	if err := lbCopy2.Pull(PullContext{Storage: db}); err != nil {
		t.Fatal(err)
	}

	if len(lbCopy2) != 4 {
		t.Fatalf("Expected to have still 4 labels but got %v instead\n", len(lbCopy2))
	}

	for _, lb := range lbCopy2 {
		if lb.Name == "Label #1" && (lb.Parent == nil || lb.Parent.Name != "Label #5") {
			t.Fatalf("Expected Label #1 to have parent Label #5 but instead has %v", lb.Parent)
		}
		if lb.Name == "Label #5" && (lb.Parent == nil || lb.Parent.Name != "Label #3") {
			t.Fatalf("Expected Label #5 to have parent Label #3 but instead has %v", lb.Parent)
		}
		if lb.Name == "Label #2" && (lb.Parent == nil || lb.Parent.Name != "Label #1") {
			t.Fatalf("Expected Label #2 to have parent Label #1 but instead has %v", lb.Parent)
		}
	}
}

func testEmptyNameChecks(t *testing.T, db *gorm.DB) {
	b := NewActor("")
	if err := (&Actors{b}).Push(PushContext{Storage: db, BatchSize: 1}); err == nil {
		t.Fatal("Expected push to fail because actor has empty name")
	}

	l := NewLabel("", nil)
	if err := (&Labels{l}).Push(PushContext{Storage: db, BatchSize: 1}); err == nil {
		t.Fatal("Expected push to fail because label has empty name")
	}
}

func TestActorsAPI_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testActorsAPI(t, db)
}

func TestLabelsAPI_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testLabelsAPI(t, db)
}

func TestTransactionsAPI_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testTransactionsAPI(t, db)
}

func TestIncorrectAmountForTransaction_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	// silent intentionally errors
	testIncorrectAmountForTransaction(t, db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}))
}

func TestPrimaryRequestsWithTransactions_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testPrimaryRequestsWithTransactions(t, db)
}

func TestPrimaryRequestsWithLabelsTree_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testPrimaryRequestsWithLabelsTree(t, db)
}

func TestTransactionJustAppendFlag_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testTransactionJustAppendFlag(t, db)
}

func TestLabelParentUpsert_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	testLabelParentUpsert(t, db)
}

func TestEmptyNameChecks_MySQL(t *testing.T) {
	db := begin(_MySQL)
	defer close(db)

	// silent intentionally errors
	testEmptyNameChecks(t, db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}))
}

func TestActorsAPI_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testActorsAPI(t, db)
}

func TestLabelsAPI_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testLabelsAPI(t, db)
}

func TestTransactionsAPI_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testTransactionsAPI(t, db)
}

func TestTransactionJustAppendFlag_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testTransactionJustAppendFlag(t, db)
}

func TestIncorrectAmountForTransaction_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	// silent intentionally errors
	testIncorrectAmountForTransaction(t, db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}))
}

func TestPrimaryRequestsWithTransactions_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testPrimaryRequestsWithTransactions(t, db)
}

func TestPrimaryRequestsWithLabelsTree_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testPrimaryRequestsWithLabelsTree(t, db)
}

func TestLabelParentUpsert_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	testLabelParentUpsert(t, db)
}

func TestEmptyNameChecks_SQLite(t *testing.T) {
	db := begin(_SQLite)
	defer close(db)

	// silent intentionally errors
	testEmptyNameChecks(t, db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}))
}

func TestIncorrectTransactions_Json(t *testing.T) {
	input := `{
	    "amount": "100",
	    "date": 31231435,
	    "sender": "?!!",
	    "receiver": "??!",
	    "label" "???"
	}`

	var trx Transactions
	if err := FromJson([]byte(input), &trx); err == nil {
		t.Fatal("Expected failure from invalid JSON but instead it worked")
	}

	if len(trx) != 0 {
		t.Fatal("Expected no transactions because of inccorect JSON input")
	}
}

func TestEncodeDecodeTransactions_Json(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2021-04-24")

	oneTransaction := Transactions{
		Transaction{
			Date:         date,
			Amount:       -100, // 1.00
			LabelName:    "?",
			SenderName:   "?",
			ReceiverName: "?",
			Details: []*Details{
				{
					LabelName: "?",
					Amount:    50,
				},
				{
					LabelName: "?",
					Amount:    30,
				},
				{
					LabelName: "?",
					Amount:    20,
				},
			},
		},
	}

	expectedJson := `[
		{
			"date": "2021-04-24T00:00:00Z",
			"amount": -100,
			"label": "?",
			"sender": "?",
			"receiver": "?",
			"flags": 0,
			"headers": "",
			"details": [
				{
					"label": "?",
					"amount": 50,
					"flags": 0,
					"headers": ""
				},
				{
					"label": "?",
					"amount": 30,
					"flags": 0,
					"headers": ""
				},
				{
					"label": "?",
					"amount": 20,
					"flags": 0,
					"headers": ""
				}
			]
		}
	]`

	if out, err := ToJson(oneTransaction); err != nil {
		t.Fatal(err)
	} else {
		var tmp Transactions
		if err := FromJson([]byte(expectedJson), &tmp); err != nil {
			t.Fatal(err)
		}
		if out2, err := ToJson(tmp); err != nil {
			t.Fatal(err)
		} else {
			if !bytes.Equal(out, out2) {
				t.Fatal("Expected JSON results to be the same")
			}
		}
	}
}

func TestEncodeDecodeLabels_Json(t *testing.T) {
	input := `[
		{
			"name": "test with null parent",
			"parent": null
		}
	]`

	var lb Labels
	if err := FromJson([]byte(input), &lb); err != nil {
		t.Fatal(err)
	}
}
