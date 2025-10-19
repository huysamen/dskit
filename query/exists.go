package query

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

func Exists(ctx context.Context, client Client, key *datastore.Key) (bool, error) {
	if err := requiresClient(client); err != nil {
		return false, err
	}

	if err := requiresKey(key); err != nil {
		return false, err
	}

	var result datastore.Entity

	err := client.Client().Get(ctx, key, &result)

	if err == datastore.ErrNoSuchEntity {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func ExistsTxn(txn Transaction, key *datastore.Key) (bool, error) {
	if err := requiresTransaction(txn); err != nil {
		return false, err
	}

	if err := requiresKey(key); err != nil {
		return false, err
	}

	var result datastore.Entity

	err := txn.Txn().Get(key, &result)

	if err == datastore.ErrNoSuchEntity {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func ExistsForQuery(ctx context.Context, client Client, query *datastore.Query) (bool, error) {
	if err := requiresClient(client); err != nil {
		return false, err
	}

	if err := requiresQuery(query); err != nil {
		return false, err
	}

	it := client.Client().Run(ctx, query.KeysOnly().Limit(1))

	_, err := it.Next(nil)

	if errors.Is(err, iterator.Done) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func ExistsForQueryTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query) (bool, error) {
	if err := requiresTransaction(txn); err != nil {
		return false, err
	}

	if err := requiresQuery(query); err != nil {
		return false, err
	}

	it := client.Client().Run(ctx, query.KeysOnly().Limit(1).Transaction(txn.Txn()))

	_, err := it.Next(nil)

	if errors.Is(err, iterator.Done) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
