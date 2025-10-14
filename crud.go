package dskit

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
)

type CRUD[E Entity] interface {
	Create(ctx context.Context, key *datastore.Key, entity *E) (*datastore.Key, error)
	CreateTxn(txn *datastore.Transaction, key *datastore.Key, entity *E) (*datastore.PendingKey, error)
	CreateMulti(ctx context.Context, keys []*datastore.Key, entities []*E) ([]*datastore.Key, error)
	CreateMultiTxn(txn *datastore.Transaction, keys []*datastore.Key, entities []*E) ([]*datastore.PendingKey, error)
	Read(ctx context.Context, key *datastore.Key) (*E, error)
	ReadTxn(txn *datastore.Transaction, key *datastore.Key) (*E, error)
	ReadMulti(ctx context.Context, keys []*datastore.Key) ([]*E, error)
	ReadMultiTxn(txn *datastore.Transaction, keys []*datastore.Key) ([]*E, error)
	List(ctx context.Context, ancestor *datastore.Key) ([]*E, *datastore.Cursor, error)
	ListTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*E, *datastore.Cursor, error)
	ListKeys(ctx context.Context, ancestor *datastore.Key) ([]*datastore.Key, *datastore.Cursor, error)
	ListKeysTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*datastore.Key, *datastore.Cursor, error)
	ListAll(ctx context.Context, ancestor *datastore.Key) ([]*E, error)
	ListAllTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*E, error)
	ListAllKeys(ctx context.Context, ancestor *datastore.Key) ([]*datastore.Key, error)
	ListAllKeysTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*datastore.Key, error)
	Update(ctx context.Context, entity *E) error
	UpdateTxn(txn *datastore.Transaction, entity *E) error
	UpdateMulti(ctx context.Context, entities []*E) error
	UpdateMultiTxn(txn *datastore.Transaction, entities []*E) error
	Delete(ctx context.Context, key *datastore.Key) error
	DeleteTxn(txn *datastore.Transaction, key *datastore.Key) error
	DeleteMulti(ctx context.Context, keys []*datastore.Key) error
	DeleteMultiTxn(txn *datastore.Transaction, keys []*datastore.Key) error
	Query(ctx context.Context, query *datastore.Query) ([]*E, *datastore.Cursor, error)
	QueryTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query) ([]*E, *datastore.Cursor, error)
}

type crud[E Entity] struct {
	client *datastore.Client
	kind   string
}

func NewCRUDRepo[E Entity](client *datastore.Client) CRUD[E] {
	var e E
	return &crud[E]{
		client: client,
		kind:   e.Kind(),
	}
}

func (r *crud[E]) Create(ctx context.Context, key *datastore.Key, entity *E) (*datastore.Key, error) {
	if key == nil {
		newKey, err := entityNewKey(entity)
		if err != nil {
			return nil, err
		}

		key = newKey
	} else {
		if _, err := requireEntity(entity); err != nil {
			return nil, err
		}
	}

	touchEntityForCreate(entity)

	return r.client.Put(ctx, key, entity)
}

func (r *crud[E]) CreateTxn(txn *datastore.Transaction, key *datastore.Key, entity *E) (*datastore.PendingKey, error) {
	if key == nil {
		newKey, err := entityNewKey(entity)
		if err != nil {
			return nil, err
		}

		key = newKey
	} else {
		if _, err := requireEntity(entity); err != nil {
			return nil, err
		}
	}

	touchEntityForCreate(entity)

	return txn.Put(key, entity)
}

func (r *crud[E]) CreateMulti(ctx context.Context, keys []*datastore.Key, entities []*E) ([]*datastore.Key, error) {
	if len(entities) == 0 {
		return nil, errors.New("entities cannot be empty")
	}

	if len(keys) == 0 {
		keys = make([]*datastore.Key, len(entities))
	} else if len(keys) != len(entities) {
		return nil, errors.New("keys and entities must have the same length")
	}

	for i, entity := range entities {
		newKey, err := entityNewKey(entity)
		if err != nil {
			return nil, fmt.Errorf("entity at index %d: %w", i, err)
		}

		if keys[i] == nil {
			keys[i] = newKey
		}

		touchEntityForCreate(entity)
	}

	return r.client.PutMulti(ctx, keys, entities)
}

func (r *crud[E]) CreateMultiTxn(txn *datastore.Transaction, keys []*datastore.Key, entities []*E) ([]*datastore.PendingKey, error) {
	if len(entities) == 0 {
		return nil, errors.New("entities cannot be empty")
	}

	if len(keys) == 0 {
		keys = make([]*datastore.Key, len(entities))
	} else if len(keys) != len(entities) {
		return nil, errors.New("keys and entities must have the same length")
	}

	for i, entity := range entities {
		newKey, err := entityNewKey(entity)
		if err != nil {
			return nil, fmt.Errorf("entity at index %d: %w", i, err)
		}

		if keys[i] == nil {
			keys[i] = newKey
		}

		touchEntityForCreate(entity)
	}

	return txn.PutMulti(keys, entities)
}

func (r *crud[E]) Read(ctx context.Context, key *datastore.Key) (*E, error) {
	var e E

	err := r.client.Get(ctx, key, &e)

	if errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (r *crud[E]) ReadTxn(txn *datastore.Transaction, key *datastore.Key) (*E, error) {
	var e E

	err := txn.Get(key, &e)

	if errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (r *crud[E]) ReadMulti(ctx context.Context, keys []*datastore.Key) ([]*E, error) {
	if len(keys) == 0 {
		return make([]*E, 0), nil
	}

	values := make([]E, len(keys))

	if err := r.client.GetMulti(ctx, keys, values); err != nil {
		return nil, err
	}

	entities := make([]*E, len(values))
	for i := range values {
		entities[i] = &values[i]
	}

	return entities, nil
}

func (r *crud[E]) ReadMultiTxn(txn *datastore.Transaction, keys []*datastore.Key) ([]*E, error) {
	if len(keys) == 0 {
		return make([]*E, 0), nil
	}

	values := make([]E, len(keys))

	if err := txn.GetMulti(keys, values); err != nil {
		return nil, err
	}

	entities := make([]*E, len(values))
	for i := range values {
		entities[i] = &values[i]
	}

	return entities, nil
}

func (r *crud[E]) List(ctx context.Context, ancestor *datastore.Key) ([]*E, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	return Query[E](ctx, r.client, query)
}

func (r *crud[E]) ListTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*E, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	return QueryTxn[E](ctx, txn, r.client, query)
}

func (r *crud[E]) ListKeys(ctx context.Context, ancestor *datastore.Key) ([]*datastore.Key, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	return QueryKeys(ctx, r.client, query)
}

func (r *crud[E]) ListKeysTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*datastore.Key, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	return QueryKeysTxn(ctx, txn, r.client, query)
}

func (r *crud[E]) ListAll(ctx context.Context, ancestor *datastore.Key) ([]*E, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := Query[E](ctx, r.client, query)

	return e, err
}

func (r *crud[E]) ListAllTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*E, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := QueryTxn[E](ctx, txn, r.client, query)

	return e, err
}

func (r *crud[E]) ListAllKeys(ctx context.Context, ancestor *datastore.Key) ([]*datastore.Key, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	keys, _, err := QueryKeys(ctx, r.client, query)

	return keys, err
}

func (r *crud[E]) ListAllKeysTxn(ctx context.Context, txn *datastore.Transaction, ancestor *datastore.Key) ([]*datastore.Key, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	keys, _, err := QueryKeysTxn(ctx, txn, r.client, query)

	return keys, err
}

func (r *crud[E]) Update(ctx context.Context, entity *E) error {
	key, err := entityKey(entity)
	if err != nil {
		return err
	}

	touchEntityForUpdate(entity)

	_, err = r.client.Put(ctx, key, entity)

	return err
}

func (r *crud[E]) UpdateTxn(txn *datastore.Transaction, entity *E) error {
	key, err := entityKey(entity)
	if err != nil {
		return err
	}

	touchEntityForUpdate(entity)

	_, err = txn.Put(key, entity)

	return err
}

func (r *crud[E]) UpdateMulti(ctx context.Context, entities []*E) error {
	if len(entities) == 0 {
		return nil
	}

	keys := make([]*datastore.Key, len(entities))

	for i, entity := range entities {
		key, err := entityKey(entity)
		if err != nil {
			return fmt.Errorf("entity at index %d: %w", i, err)
		}

		keys[i] = key

		touchEntityForUpdate(entity)
	}

	_, err := r.client.PutMulti(ctx, keys, entities)

	return err
}

func (r *crud[E]) UpdateMultiTxn(txn *datastore.Transaction, entities []*E) error {
	if len(entities) == 0 {
		return nil
	}

	keys := make([]*datastore.Key, len(entities))

	for i, entity := range entities {
		key, err := entityKey(entity)
		if err != nil {
			return fmt.Errorf("entity at index %d: %w", i, err)
		}

		keys[i] = key

		touchEntityForUpdate(entity)
	}

	_, err := txn.PutMulti(keys, entities)

	return err
}

func (r *crud[E]) Delete(ctx context.Context, key *datastore.Key) error {
	return r.client.Delete(ctx, key)
}

func (r *crud[E]) DeleteTxn(txn *datastore.Transaction, key *datastore.Key) error {
	return txn.Delete(key)
}

func (r *crud[E]) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	return r.client.DeleteMulti(ctx, keys)
}

func (r *crud[E]) DeleteMultiTxn(txn *datastore.Transaction, keys []*datastore.Key) error {
	return txn.DeleteMulti(keys)
}

func (r *crud[E]) Query(ctx context.Context, query *datastore.Query) ([]*E, *datastore.Cursor, error) {
	if query == nil {
		query = datastore.NewQuery(r.kind)
	}

	return Query[E](ctx, r.client, query)
}

func (r *crud[E]) QueryTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query) ([]*E, *datastore.Cursor, error) {
	if query == nil {
		query = datastore.NewQuery(r.kind)
	}

	return QueryTxn[E](ctx, txn, r.client, query)
}

func requireEntity[E Entity](entity *E) (*E, error) {
	if entity == nil {
		return nil, errors.New("entity cannot be nil")
	}

	return entity, nil
}

func entityNewKey[E Entity](entity *E) (*datastore.Key, error) {
	if _, err := requireEntity(entity); err != nil {
		return nil, err
	}

	return (*entity).NewKey(), nil
}

func entityKey[E Entity](entity *E) (*datastore.Key, error) {
	if _, err := requireEntity(entity); err != nil {
		return nil, err
	}

	return (*entity).Key(), nil
}
