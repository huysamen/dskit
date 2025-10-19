package query

import "cloud.google.com/go/datastore"

type Client interface {
	Client() *datastore.Client
}

type Transaction interface {
	Txn() *datastore.Transaction
}
