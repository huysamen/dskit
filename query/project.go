package query

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

func Project[E any](ctx context.Context, client Client, query *datastore.Query, generate Generator[E], fields ...string) ([]*E, *datastore.Cursor, error) {
	if err := requiresClient(client); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	it := client.Client().Run(ctx, query.Project(fields...))
	entities := make([]*E, 0, defaultQueryAllocationSize)

	for {
		e := generate()

		_, err := it.Next(e)
		if errors.Is(err, iterator.Done) {
			break
		}

		var efm *datastore.ErrFieldMismatch

		if errors.As(err, &efm) {
			entities = append(entities, e)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		entities = append(entities, e)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return entities, &c, nil
}

func ProjectTxn[E any](ctx context.Context, txn Transaction, client Client, query *datastore.Query, generate Generator[E], fields ...string) ([]*E, *datastore.Cursor, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	it := client.Client().Run(ctx, query.Project(fields...).Transaction(txn.Txn()))
	entities := make([]*E, 0, defaultQueryAllocationSize)

	for {
		e := generate()

		_, err := it.Next(e)
		if errors.Is(err, iterator.Done) {
			break
		}

		var efm *datastore.ErrFieldMismatch

		if errors.As(err, &efm) {
			entities = append(entities, e)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		entities = append(entities, e)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return entities, &c, nil
}

func ProjectOne[E any](ctx context.Context, client Client, query *datastore.Query, generate Generator[E], fields ...string) (*E, error) {
	entities, _, err := Project(ctx, client, query, generate, fields...)
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return nil, nil
	}

	return entities[0], nil
}

func ProjectOneTxn[E any](ctx context.Context, txn Transaction, client Client, query *datastore.Query, generate Generator[E], fields ...string) (*E, error) {
	entities, _, err := ProjectTxn(ctx, txn, client, query, generate, fields...)
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return nil, nil
	}

	return entities[0], nil
}
