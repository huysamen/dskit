package query

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

func Query[E any](ctx context.Context, client Client, query *datastore.Query) ([]*E, *datastore.Cursor, error) {
	if err := requiresClient(client); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	it := client.Client().Run(ctx, query)
	entities := make([]*E, 0, defaultQueryAllocationSize)

	for {
		var e E

		_, err := it.Next(&e)
		if errors.Is(err, iterator.Done) {
			break
		}

		var efm *datastore.ErrFieldMismatch

		if errors.As(err, &efm) {
			entities = append(entities, &e)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		entities = append(entities, &e)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return entities, &c, nil
}

func QueryTxn[E any](ctx context.Context, txn Transaction, client Client, query *datastore.Query) ([]*E, *datastore.Cursor, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	query = query.Transaction(txn.Txn())

	it := client.Client().Run(ctx, query)
	entities := make([]*E, 0, defaultQueryAllocationSize)

	for {
		var e E

		_, err := it.Next(&e)
		if errors.Is(err, iterator.Done) {
			break
		}

		var efm *datastore.ErrFieldMismatch

		if errors.As(err, &efm) {
			entities = append(entities, &e)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		entities = append(entities, &e)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return entities, &c, nil
}

func QueryKeys(ctx context.Context, client Client, query *datastore.Query) ([]*datastore.Key, *datastore.Cursor, error) {
	if err := requiresClient(client); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	it := client.Client().Run(ctx, query.KeysOnly())
	keys := make([]*datastore.Key, 0, defaultQueryAllocationSize)

	for {
		k, err := it.Next(nil)
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		keys = append(keys, k)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return keys, &c, nil
}

func QueryKeysTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query) ([]*datastore.Key, *datastore.Cursor, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	it := client.Client().Run(ctx, query.KeysOnly().Transaction(txn.Txn()))
	keys := make([]*datastore.Key, 0, defaultQueryAllocationSize)

	for {
		k, err := it.Next(nil)
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		keys = append(keys, k)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return keys, &c, nil
}

func QueryOne[E any](ctx context.Context, client Client, query *datastore.Query) (*E, error) {
	entities, _, err := Query[E](ctx, client, query.Limit(1))
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return nil, datastore.ErrNoSuchEntity
	}

	return entities[0], nil
}

func QueryOneTxn[E any](ctx context.Context, txn Transaction, client Client, query *datastore.Query) (*E, error) {
	entities, _, err := QueryTxn[E](ctx, txn, client, query.Limit(1))
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return nil, datastore.ErrNoSuchEntity
	}

	return entities[0], nil
}

func QueryOneKey(ctx context.Context, client Client, query *datastore.Query) (*datastore.Key, error) {
	keys, _, err := QueryKeys(ctx, client, query.KeysOnly().Limit(1))
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, datastore.ErrNoSuchEntity
	}

	return keys[0], nil
}

func QueryOneKeyTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query) (*datastore.Key, error) {
	keys, _, err := QueryKeysTxn(ctx, txn, client, query.KeysOnly().Limit(1))
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, datastore.ErrNoSuchEntity
	}

	return keys[0], nil
}
