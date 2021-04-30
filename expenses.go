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
// THE SOFTWARE.
package expenses

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ModVersion = "0.1.0"
	DateFormat = "Mon 02 Jan 2006" // Layout used instead of "d m Y" abbr
)

// Registry is defined as an unified simplistic API developed to interact
// with an underlaying persistence layer via only two routes. The concept
// of the expenses module is visualy defined by its component parts below
//
//    (Participants)
//        Actor        Label (User defined label to identify scope)
//          |            |
//          \           /|
//           Transaction |
//                \_    /
//                   Details (Transaction breakdown for the amount)
//
// A *Transaction* is a registry entry that demonstrates an exchange between
// two participants (Actors) for a given amount (base currency) with one or
// many declared scopes (Transaction Label and Details Labels)
//
// Only *collections* of records are considered valid candidates to implement
// this interface since the concept of the registry works with sets of items
type Registry interface {
	Push(PushContext) error
	Pull(PullContext) error
}

// PushContext is a thin wrapper to "explain" to an entity what needs to be
// pushed into registry since the registry it's not aware of the underlying
// persistence layer
//
// The use of PUSH is to create records through upsert and update fields on
// conflict
type PushContext struct {

	// Storage is mainly Maria/MySQL with little support for SQLite
	Storage *gorm.DB

	// BatchSize must be a positive number above zero, otherwise it
	// will fail to write
	BatchSize int

	// JustAppend is mostly used internally to upsert only and don't
	// propage updates to all fields
	JustAppend bool
}

// PullContext is complement with PushContext (see above)
//
// The use of PULL is to retrieve records
type PullContext struct {

	// Storage is mainly Maria/MySQL with little support for SQLite
	Storage *gorm.DB

	// Limit is the equivalent of SQL LIMIT statement. By default it
	// isn't set on query because of the default zero value
	Limit int

	// Offset is the equivalent of SQL OFFSET statement on query. It
	// is similar to Limit, by default it isn't set
	Offset int
}

// NullString is a compatible SQL and JSON structure, mostly added
// to support the tree structure of Label's optional Parent
type NullString struct {
	sql.NullString
}

// UnmarshalJSON to properly support NULL conversion (the problem
// was actually sql.NullString not having this conversion)
func (ns *NullString) UnmarshalJSON(b []byte) error {
	ns.Valid = string(b) != "null"

	return json.Unmarshal(b, &ns.String)
}

// MarshalJSON to properly support NULL conversion (see above)
func (ns NullString) MarshalJSON() ([]byte, error) {
	if ns.Valid {
		return json.Marshal(ns.String)
	}

	return json.Marshal(nil)
}

// Actors is a registry-type that represents a collection of its
// appropriate *Actor* entities
type Actors []Actor

// Push enables to write new actors into registry or updates the
// fields of the existing ones if a *name* conflict occurs
func (a *Actors) Push(ctx PushContext) error {
	q := ctx.Storage

	if ctx.JustAppend {
		q = q.Clauses(clause.OnConflict{DoNothing: true})
	} else {
		q = q.Clauses(clause.OnConflict{UpdateAll: true})
	}

	return q.CreateInBatches(a, ctx.BatchSize).Error
}

// Pull enables to read actors from registry. The results are
// always sorted by their name
func (a *Actors) Pull(ctx PullContext) error {
	q := ctx.Storage.Order("Name").Limit(ctx.Limit).Offset(ctx.Offset)

	return q.Find(a).Error
}

// Actor is one of the key components of the expenses module. An actor
// is an abstraction of any participant in a transaction. Currenly its
// use is to differenciate between *senders* and *receivers*
type Actor struct {
	Name      string    `json:"name" gorm:"type: varchar(100); primaryKey"`
	Flags     uint16    `json:"flags" gorm:"not null"`
	Headers   string    `json:"headers" gorm:"type: text; not null"`
	CreatedAt time.Time `json:"-" gorm:"autoCreateTime"`
	UpdatedAt time.Time `json:"-" gorm:"autoUpdateTime"`
}

// String representation of an *actor* (any actor; this output won't
// tell whether it's a sender or a receiver because this role exists
// only in the context of a transaction)
func (a *Actor) String() string {
	return fmt.Sprintf(`A{Name=%s}`, a.Name)
}

// BeforeCreate hook from GORM to check if actors has valid name
func (a *Actor) BeforeCreate(tx *gorm.DB) (err error) {
	if a.Name == "" {
		return fmt.Errorf("Actor cannot have have an empty name")
	}

	return
}

// Labels is a registry-type that represents a collection of its
// appropriate *Label* entities
type Labels []Label

// Push enables to write new labels into registry or updates the
// fields of the existing ones if a *name* conflict occurs. Each
// label can have a parent label to link with
func (l *Labels) Push(ctx PushContext) error {
	distincts := make(map[string]Label)
	for i, lb := range *l {
		var parent *Label
		for parent = lb.Parent; parent != nil; parent = parent.Parent {
			if _, ok := distincts[parent.Name]; !ok {
				distincts[parent.Name] = *parent
			}
		}

		if _, ok := distincts[lb.Name]; !ok {
			distincts[lb.Name] = lb
		}

		// sync pointer to Labels with ParentName attached by gorm
		if lb.Parent != nil {
			(*l)[i].ParentName = NullString{
				sql.NullString{String: lb.Parent.Name, Valid: true},
			}
		}
	}

	list := make([]Label, 0, len(distincts))
	for _, lb := range distincts {
		list = append(list, lb)
	}

	q := ctx.Storage
	if ctx.JustAppend {
		q = q.Clauses(clause.OnConflict{DoNothing: true})
	} else {
		q = q.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "name"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"parent_name", "flags", "headers", "updated_at",
			}),
		})
	}

	return q.CreateInBatches(&list, ctx.BatchSize).Error
}

// Pull enables to read labels from registry. The results are
// always sorted by their name and contain the parents of the
// already retrieved labels as well
func (l *Labels) Pull(ctx PullContext) error {
	q := ctx.Storage.Preload("Parent")

	return q.Limit(ctx.Limit).Offset(ctx.Offset).Order("Name").Find(l).Error
}

// Label is another key component of the expenses module. A label is
// an user-defined entity used to classify transactions through meta
// information. The label has a tree-like structure where any entity
// can be the parent of any other entities, while the root entity is
// always present with NULL value for parent field
type Label struct {
	Name       string     `json:"name" gorm:"type: varchar(100); primaryKey"`
	ParentName NullString `json:"parent" gorm:"type: varchar(100)"`
	Flags      uint16     `json:"flags" gorm:"not null"`
	Headers    string     `json:"headers" gorm:"type: text; not null"`
	CreatedAt  time.Time  `json:"-" gorm:"autoCreateTime"`
	UpdatedAt  time.Time  `json:"-" gorm:"autoUpdateTime"`

	Parent *Label `json:"-" gorm:"foreignKey: ParentName"`
}

// String representation of a *label* (any label)
func (lb *Label) String() string {
	return fmt.Sprintf(`L{Name=%s Parent=%v}`, lb.Name, lb.Parent)
}

// BeforeCreate hook from GORM to correctly attach the parent name from
// the relationship, if any; and validate provided name
func (lb *Label) BeforeCreate(tx *gorm.DB) (err error) {
	if lb.Name == "" {
		return fmt.Errorf("Label cannot have an empty name")
	}

	if lb.Parent != nil {
		lb.ParentName = NullString{
			sql.NullString{String: lb.Parent.Name, Valid: true},
		}
	}

	return
}

// Transactions is a registry-type that represents a collection of its
// appropriate *Transaction* entities. This is the primary registry of
// the expenses module
type Transactions []Transaction

// Push into registry *must* always succeed to write a list of transactions
// in the persistent layer, whether it requires additional Actors/Labels to
// be written before the actual commit or to add details after the commit
func (t *Transactions) Push(ctx PushContext) error {
	seenActors := make(map[string]bool)
	everyActor := Actors{}
	catchActor := func(a Actor) {
		if _, ok := seenActors[a.Name]; !ok && a.Name != "" {
			seenActors[a.Name] = true
			everyActor = append(everyActor, a)
		}
	}

	seenLabels := make(map[string]bool)
	everyLabel := Labels{}
	catchLabel := func(l Label) {
		if _, ok := seenLabels[l.Name]; !ok && l.Name != "" {
			seenLabels[l.Name] = true
			everyLabel = append(everyLabel, l)

			var parent *Label
			parent = l.Parent

			for parent != nil {
				everyLabel = append(everyLabel, *parent)
				parent = parent.Parent
			}
		}
	}

	for _, trx := range *t {
		if trx.Receiver != nil {
			catchActor(*trx.Receiver)
		} else if trx.ReceiverName != "" {
			catchActor(Actor{Name: trx.ReceiverName})
		}

		if trx.Sender != nil {
			catchActor(*trx.Sender)
		} else if trx.SenderName != "" {
			catchActor(Actor{Name: trx.SenderName})
		}

		if trx.Label != nil {
			catchLabel(*trx.Label)
		} else if trx.LabelName != "" {
			catchLabel(Label{Name: trx.LabelName})
		}

		for _, ls := range trx.Details {
			if ls.Label != nil {
				catchLabel(*ls.Label)
			} else if ls.LabelName != "" {
				catchLabel(Label{Name: ls.LabelName})
			}
		}
	}

	subctx := ctx
	subctx.JustAppend = true

	if err := everyActor.Push(subctx); err != nil {
		return err
	}

	if err := everyLabel.Push(subctx); err != nil {
		return err
	}

	q := ctx.Storage
	if ctx.JustAppend {
		q = q.Clauses(clause.OnConflict{DoNothing: true})
	} else {
		q = q.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "uuid"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"label_name", "sender_name", "receiver_name",
				"flags", "headers", "updated_at",
			}),
		})
	}

	return q.CreateInBatches(t, ctx.BatchSize).Error
}

// Pull from registry automatically resolves the relationship between these
// three components (Actors, Labels, Details) and the results are sorted by
// the *date* of the real-world transaction authorization
func (t *Transactions) Pull(ctx PullContext) error {
	q := ctx.Storage.Preload("Details.Label").Preload(clause.Associations)

	return q.Limit(ctx.Limit).Offset(ctx.Offset).Order("Date").Find(t).Error
}

// Transaction *is* the key component of the expenses module which bounds
// together foreign Actors and Labels. Any transaction entity is actually
// the equivalent of a real-world transaction between two parties, namely
// a Sender and a Receiver, which are both Actors. Meta information about
// the transaction itself is made through user-defined Labels
//
// A transaction cannot change its *date* and payment *amount* afterwards
// but it allows to update its Actors and Labels, as long as they exists;
// and most importantly: the *amount* field is used to interpret the type
// of the transaction as a binary operation (IN > 0 otherwise OUT)
type Transaction struct {
	UUID         *string   `json:"uuid,omitempty" gorm:"type: varchar(36); primaryKey"`
	Date         time.Time `json:"date" gorm:"type: date; index; not null"`
	Amount       int64     `json:"amount" gorm:"not null"`
	LabelName    string    `json:"label" gorm:"not null"`
	SenderName   string    `json:"sender" gorm:"not null"`
	ReceiverName string    `json:"receiver" gorm:"not null"`
	Flags        uint16    `json:"flags" gorm:"not null"`
	Headers      string    `json:"headers" gorm:"type: text; not null"`
	CreatedAt    time.Time `json:"-" gorm:"autoCreateTime"`
	UpdatedAt    time.Time `json:"-" gorm:"autoUpdateTime"`

	Label    *Label `json:"-" gorm:"foreignKey: LabelName"`
	Sender   *Actor `json:"-" gorm:"foreignKey: SenderName"`
	Receiver *Actor `json:"-" gorm:"foreignKey: ReceiverName"`

	Details []*Details `json:"details" gorm:"foreignKey: TransactionUUID; references: UUID"`
}

// String representation of a transaction with all its fields and ahead
// of time resolution of the relationship between components. An output
// may contain transaction details as well
func (t *Transaction) String() string {
	return fmt.Sprintf(`T{Date=%s Amount=%d Label=%v Sender=%v Receiver=%v Details=%v}`,
		t.Date.Format(DateFormat), t.Amount, t.Label, t.Sender, t.Receiver, t.Details)
}

// BeforeCreate hook from GORM to generate an UUID just before creating
// the new entry for the transaction. The use of UUID as string instead
// of binary is due to JSON (un)marshal and portability over ASCII only
// communication channels
//
// This method is responsible for constraints check upon amount details
// preventing the introduction of incomplete or corrupted transactions
func (t *Transaction) BeforeCreate(tx *gorm.DB) (err error) {
	if t.UUID == nil {
		pk := uuid.New().String()
		t.UUID = &pk
	}

	if len(t.Details) > 0 {
		var sum int64
		for _, d := range t.Details {
			sum += d.Amount
		}

		amount := t.Amount
		if amount < 0 {
			amount *= -1
		}

		if sum != amount {
			return fmt.Errorf("transaction details don't add up, expected %d but got %d", amount, sum)
		}
	}

	return
}

// Details is an adjacent component of the expenses module to support the
// *amount* breakdown of a Transaction into multiple records individually
// labeled.
//
// This is the *only* entity that's created indirectly from a Transaction
// and cannot have its fields updated in any way
type Details struct {
	UUID            *string   `json:"-" gorm:"type: varchar(36); primaryKey"`
	TransactionUUID string    `json:"-" gorm:"not null"`
	LabelName       string    `json:"label" gorm:"not null"`
	Amount          int64     `json:"amount" gorm:"not null"`
	Flags           uint16    `json:"flags" gorm:"not null"`
	Headers         string    `json:"headers" gorm:"type: text; not null"`
	CreatedAt       time.Time `json:"-" gorm:"autoCreateTime"`
	UpdatedAt       time.Time `json:"-" gorm:"autoUpdateTime"`

	Transaction *Transaction `json:"-" gorm:"foreignKey: TransactionUUID"`
	Label       *Label       `json:"-" gorm:"foreignKey: LabelName"`
}

// String representation of transaction's detailed entity
func (d *Details) String() string {
	return fmt.Sprintf(`D{Amount=%d Label=%v}`, d.Amount, d.Label)
}

// BeforeCreate hook from GORM to generate an UUID just before creating
// the new entry for the transaction details
func (d *Details) BeforeCreate(tx *gorm.DB) (err error) {
	if d.UUID == nil {
		pk := uuid.New().String()
		d.UUID = &pk
	}

	if d.Amount < 0 {
		return errors.New("details amount cannot be negative")
	}

	return
}

// FromJson is a tiny helper function to deserialize a JSON payload into
// one of the registry key components (Actors, Labels, Transactions)
func FromJson(src []byte, into interface{}) error {
	return json.Unmarshal(src, into)
}

// ToJson is a tiny helper function to serialize into JSON any supported
// registry key components (this is the inverse of FromJson)
func ToJson(src interface{}) ([]byte, error) {
	if bytez, err := json.Marshal(src); err != nil {
		return nil, err
	} else {
		return bytez, nil
	}
}

// tables are declared to be used for Install and Uninstall
var tables = [...]interface{}{
	&Actor{},
	&Label{},
	&Transaction{},
	&Details{},
}

// Install is a helper function to create and migrate the required tables
// on a supported database. Failure to install returns errors and must be
// handled by the caller
func Install(db *gorm.DB) error {
	for _, table := range tables {
		if err := db.AutoMigrate(table); err != nil {
			return err
		}
	}

	return nil
}

// Uninstall is a helper function to delete previous installments. Upon
// failure it returns errors that must be handled by the caller
func Uninstall(db *gorm.DB) error {
	for _, table := range tables {
		if err := db.Migrator().DropTable(table); err != nil {
			return err
		}
	}

	return nil
}

// NewActor is an idiomatic constructor for the Actor entity. This method
// doesn't handle meta fields such as Flags or Headers
func NewActor(name string) Actor {
	return Actor{Name: name}
}

// NewLabel is an idiomatic constructor for the Label entity. This method
// doesn't handle meta fields such as Flags or Headers
func NewLabel(name string, parent *Label) Label {
	return Label{Name: name, Parent: parent}
}

// NewTransaction is the primary idiomatic constructor for the Transaction
// entity from which all other records can derive. Transaction details can
// be omitted by providing nil map. This method doesn't handle meta fields
// such as Flags or Headers
func NewTransaction(d time.Time, a int64, lb Label, tx, rx Actor, ls map[Label]int64) Transaction {
	t := Transaction{Date: d, Amount: a, Label: &lb, Sender: &tx, Receiver: &rx}

	if ls != nil {
		t.Details = make([]*Details, 0, len(ls))
		for label, value := range ls {
			t.Details = append(t.Details, &Details{Label: &label, Amount: value})
		}
	}

	return t
}

// NewPushRequest is promoted as the primary entrypoint to use/handle supported
// registry component (Actors, Labels, Transactions) because of it's wrapped on
// the repetitive pipeline to create new records and obtain a copy of the items
// in the form of Golang structs and JSON as well. Upon failure an error yields
// instead and none or incomplete records are actually being *pushed*
func NewPushRequest(reg Registry, ctx PushContext) (out []byte, err error) {
	if err = reg.Push(ctx); err != nil {
		return
	} else if out, err = ToJson(reg); err != nil {
		return
	}

	return
}

// NewPullRequest is promoted as the primary entrypoint to use/handle supported
// registry component (Actors, Labels, Transactions) because of it's wrapped on
// the repetitive pipeline to list existing records; obtain a copy of the items
// in the form of Golang structs and JSON as well. Upon failure an error yields
// instead and none or incomplete records are actually being *pulled*
func NewPullRequest(reg Registry, ctx PullContext) (out []byte, err error) {
	if err = reg.Pull(ctx); err != nil {
		return
	} else if out, err = ToJson(reg); err != nil {
		return
	}

	return
}
