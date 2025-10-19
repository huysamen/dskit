package query

import (
	"context"

	"cloud.google.com/go/datastore"
)

func Read[E any](ctx context.Context, client Client, key *datastore.Key, entity *E) error {
	if err := requiresClient(client); err != nil {
		return err
	}

	if err := requiresKey(key); err != nil {
		return err
	}

	if err := requiresEntity(entity); err != nil {
		return err
	}

	return client.Client().Get(ctx, key, entity)
}

func ReadTxn[E any](txn Transaction, key *datastore.Key, entity *E) error {
	if err := requiresTransaction(txn); err != nil {
		return err
	}

	if err := requiresKey(key); err != nil {
		return err
	}

	if err := requiresEntity(entity); err != nil {
		return err
	}

	return txn.Txn().Get(key, entity)
}

func ReadMulti[E any](ctx context.Context, client Client, keys []*datastore.Key, entities []*E) error {
	if err := requiresClient(client); err != nil {
		return err
	}

	if err := requiresEqualLength(keys, entities); err != nil {
		return err
	}

	if err := requiresKeys(keys); err != nil {
		return err
	}

	if err := requiresEntities(entities); err != nil {
		return err
	}

	return client.Client().GetMulti(ctx, keys, entities)
}

func ReadMultiTxn[E any](txn Transaction, keys []*datastore.Key, entities []*E) error {
	if err := requiresTransaction(txn); err != nil {
		return err
	}

	if err := requiresEqualLength(keys, entities); err != nil {
		return err
	}

	if err := requiresKeys(keys); err != nil {
		return err
	}

	if err := requiresEntities(entities); err != nil {
		return err
	}

	return txn.Txn().GetMulti(keys, entities)
}
