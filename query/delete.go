package query

import (
	"context"

	"cloud.google.com/go/datastore"
)

func Delete(ctx context.Context, client Client, key *datastore.Key) error {
	if err := requiresClient(client); err != nil {
		return err
	}

	if err := requiresKey(key); err != nil {
		return err
	}

	return client.Client().Delete(ctx, key)
}

func DeleteTxn(txn Transaction, key *datastore.Key) error {
	if err := requiresTransaction(txn); err != nil {
		return err
	}

	if err := requiresKey(key); err != nil {
		return err
	}

	return txn.Txn().Delete(key)
}

func DeleteMulti(ctx context.Context, client Client, keys []*datastore.Key) error {
	if err := requiresClient(client); err != nil {
		return err
	}

	if err := requiresKeys(keys); err != nil {
		return err
	}

	return client.Client().DeleteMulti(ctx, keys)
}

func DeleteMultiTxn(txn Transaction, keys []*datastore.Key) error {
	if err := requiresTransaction(txn); err != nil {
		return err
	}

	if err := requiresKeys(keys); err != nil {
		return err
	}

	return txn.Txn().DeleteMulti(keys)
}
