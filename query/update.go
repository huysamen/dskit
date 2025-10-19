package query

import (
	"context"

	"cloud.google.com/go/datastore"
)

func Update[E any](ctx context.Context, client Client, key *datastore.Key, entity *E) (*datastore.Key, error) {
	if err := requiresClient(client); err != nil {
		return nil, err
	}

	if err := requiresKey(key); err != nil {
		return nil, err
	}

	if err := requiresEntity(entity); err != nil {
		return nil, err
	}

	return client.Client().Put(ctx, key, entity)
}

func UpdateTxn[E any](txn Transaction, key *datastore.Key, entity *E) (*datastore.PendingKey, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, err
	}

	if err := requiresKey(key); err != nil {
		return nil, err
	}

	if err := requiresEntity(entity); err != nil {
		return nil, err
	}

	return txn.Txn().Put(key, entity)
}

func UpdateMulti[E any](ctx context.Context, client Client, keys []*datastore.Key, entities []*E) ([]*datastore.Key, error) {
	if err := requiresClient(client); err != nil {
		return nil, err
	}

	if err := requiresEqualLength(keys, entities); err != nil {
		return nil, err
	}

	if err := requiresKeys(keys); err != nil {
		return nil, err
	}

	if err := requiresEntities(entities); err != nil {
		return nil, err
	}

	return client.Client().PutMulti(ctx, keys, entities)
}

func UpdateMultiTxn[E any](txn Transaction, keys []*datastore.Key, entities []*E) ([]*datastore.PendingKey, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, err
	}

	if err := requiresEqualLength(keys, entities); err != nil {
		return nil, err
	}

	if err := requiresKeys(keys); err != nil {
		return nil, err
	}

	if err := requiresEntities(entities); err != nil {
		return nil, err
	}

	return txn.Txn().PutMulti(keys, entities)
}
