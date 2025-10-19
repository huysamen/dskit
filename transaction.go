package dskit

import "cloud.google.com/go/datastore"

type Transaction interface {
	Txn() *datastore.Transaction
	Commit() (*datastore.Commit, error)
	Rollback() error
}

type txn struct {
	tx *datastore.Transaction
}

func NewTransaction(tx *datastore.Transaction) Transaction {
	return &txn{tx: tx}
}

func (t *txn) Txn() *datastore.Transaction {
	return t.tx
}

func (t *txn) Commit() (*datastore.Commit, error) {
	return t.tx.Commit()
}

func (t *txn) Rollback() error {
	return t.tx.Rollback()
}
