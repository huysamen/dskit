package dskit

import (
	"context"

	"cloud.google.com/go/datastore"
	q "github.com/huysamen/dskit/query"
)

type Repo[E any] interface {
	Client() Client
	Create(ctx context.Context, ancestor *datastore.Key, entity *E) (*datastore.Key, error)
	CreateTxn(txn q.Transaction, ancestor *datastore.Key, entity *E) (*datastore.PendingKey, error)
	CreateWithKey(ctx context.Context, key *datastore.Key, entity *E) (*datastore.Key, error)
	CreateWithKeyTxn(txn q.Transaction, key *datastore.Key, entity *E) (*datastore.PendingKey, error)
	CreateMulti(ctx context.Context, ancestor *datastore.Key, entities []*E) ([]*datastore.Key, error)
	CreateMultiTxn(txn q.Transaction, ancestor *datastore.Key, entities []*E) ([]*datastore.PendingKey, error)
	CreateMultiWithKeys(ctx context.Context, keys []*datastore.Key, entities []*E) ([]*datastore.Key, error)
	CreateMultiWithKeysTxn(txn q.Transaction, keys []*datastore.Key, entities []*E) ([]*datastore.PendingKey, error)
	Read(ctx context.Context, key *datastore.Key) (*E, error)
	ReadTxn(txn q.Transaction, key *datastore.Key) (*E, error)
	ReadMulti(ctx context.Context, keys []*datastore.Key) ([]*E, error)
	ReadMultiTxn(txn q.Transaction, keys []*datastore.Key) ([]*E, error)
	List(ctx context.Context, ancestor *datastore.Key, limit int, cursor string) ([]*E, *datastore.Cursor, error)
	ListTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, cursor string) ([]*E, *datastore.Cursor, error)
	ListPage(ctx context.Context, ancestor *datastore.Key, limit int, offset int) ([]*E, *datastore.Cursor, error)
	ListPageTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, offset int) ([]*E, *datastore.Cursor, error)
	ListKeys(ctx context.Context, ancestor *datastore.Key, limit int, cursor string) ([]*datastore.Key, *datastore.Cursor, error)
	ListKeysTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, cursor string) ([]*datastore.Key, *datastore.Cursor, error)
	ListAll(ctx context.Context, ancestor *datastore.Key) ([]*E, error)
	ListAllTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key) ([]*E, error)
	ListAllKeys(ctx context.Context, ancestor *datastore.Key) ([]*datastore.Key, error)
	ListAllKeysTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key) ([]*datastore.Key, error)
	ListProjection(ctx context.Context, ancestor *datastore.Key, limit int, cursor string, generate q.Generator[any], fields ...string) ([]*any, error)
	ListProjectionTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, generate q.Generator[any], fields ...string) ([]*any, error)
	ListPageProjection(ctx context.Context, ancestor *datastore.Key, limit int, offset int, generate q.Generator[any], fields ...string) ([]*any, *datastore.Cursor, error)
	ListPageProjectionTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, offset int, generate q.Generator[any], fields ...string) ([]*any, *datastore.Cursor, error)
	ListAllProjection(ctx context.Context, ancestor *datastore.Key, generate q.Generator[any], fields ...string) ([]*any, error)
	ListAllProjectionTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, generate q.Generator[any], fields ...string) ([]*any, error)
	Update(ctx context.Context, key *datastore.Key, entity *E) error
	UpdateTxn(txn q.Transaction, key *datastore.Key, entity *E) error
	UpdateMulti(ctx context.Context, keys []*datastore.Key, entities []*E) error
	UpdateMultiTxn(txn q.Transaction, keys []*datastore.Key, entities []*E) error
	Delete(ctx context.Context, key *datastore.Key) error
	DeleteTxn(txn q.Transaction, key *datastore.Key) error
	DeleteMulti(ctx context.Context, keys []*datastore.Key) error
	DeleteMultiTxn(txn q.Transaction, keys []*datastore.Key) error
}

type CRUD[E any] = Repo[E]

type repo[E any] struct {
	client Client
	kind   string
}

func NewCRUDRepo[E any](client Client, kind string) Repo[E] {
	return &repo[E]{
		client: client,
		kind:   kind,
	}
}

func (r *repo[E]) Client() Client {
	return r.client
}

func (r *repo[E]) Create(ctx context.Context, ancestor *datastore.Key, entity *E) (*datastore.Key, error) {
	return q.Create(ctx, r.client, datastore.IncompleteKey(r.kind, ancestor), entity)
}

func (r *repo[E]) CreateTxn(txn q.Transaction, ancestor *datastore.Key, entity *E) (*datastore.PendingKey, error) {
	return q.CreateTxn(txn, datastore.IncompleteKey(r.kind, ancestor), entity)
}

func (r *repo[E]) CreateWithKey(ctx context.Context, key *datastore.Key, entity *E) (*datastore.Key, error) {
	return q.Create(ctx, r.client, key, entity)
}

func (r *repo[E]) CreateWithKeyTxn(txn q.Transaction, key *datastore.Key, entity *E) (*datastore.PendingKey, error) {
	return q.CreateTxn(txn, key, entity)
}

func (r *repo[E]) CreateMulti(ctx context.Context, ancestor *datastore.Key, entities []*E) ([]*datastore.Key, error) {
	if len(entities) == 0 {
		return make([]*datastore.Key, 0), nil
	}

	keys := make([]*datastore.Key, len(entities))

	for i := range entities {
		keys[i] = datastore.IncompleteKey(r.kind, ancestor)
	}

	return q.CreateMulti(ctx, r.client, keys, entities)
}

func (r *repo[E]) CreateMultiTxn(txn q.Transaction, ancestor *datastore.Key, entities []*E) ([]*datastore.PendingKey, error) {
	if len(entities) == 0 {
		return make([]*datastore.PendingKey, 0), nil
	}

	keys := make([]*datastore.Key, len(entities))

	for i := range entities {
		keys[i] = datastore.IncompleteKey(r.kind, ancestor)
	}

	return q.CreateMultiTxn(txn, keys, entities)
}

func (r *repo[E]) CreateMultiWithKeys(ctx context.Context, keys []*datastore.Key, entities []*E) ([]*datastore.Key, error) {
	if len(keys) == 0 {
		return make([]*datastore.Key, 0), nil
	}

	return q.CreateMulti(ctx, r.client, keys, entities)
}

func (r *repo[E]) CreateMultiWithKeysTxn(txn q.Transaction, keys []*datastore.Key, entities []*E) ([]*datastore.PendingKey, error) {
	if len(keys) == 0 {
		return make([]*datastore.PendingKey, 0), nil
	}

	return q.CreateMultiTxn(txn, keys, entities)
}

func (r *repo[E]) Read(ctx context.Context, key *datastore.Key) (*E, error) {
	entity := new(E)
	err := q.Read(ctx, r.client, key, entity)

	return entity, err
}

func (r *repo[E]) ReadTxn(txn q.Transaction, key *datastore.Key) (*E, error) {
	entity := new(E)
	err := q.ReadTxn(txn, key, entity)

	return entity, err
}

func (r *repo[E]) ReadMulti(ctx context.Context, keys []*datastore.Key) ([]*E, error) {
	if len(keys) == 0 {
		return make([]*E, 0), nil
	}

	entities := make([]*E, len(keys))
	for i := range entities {
		entities[i] = new(E)
	}

	if err := r.client.Client().GetMulti(ctx, keys, entities); err != nil {
		return nil, err
	}

	return entities, nil
}

func (r *repo[E]) ReadMultiTxn(txn q.Transaction, keys []*datastore.Key) ([]*E, error) {
	if len(keys) == 0 {
		return make([]*E, 0), nil
	}

	entities := make([]*E, len(keys))
	for i := range entities {
		entities[i] = new(E)
	}

	if err := txn.Txn().GetMulti(keys, entities); err != nil {
		return nil, err
	}

	return entities, nil
}

func (r *repo[E]) List(ctx context.Context, ancestor *datastore.Key, limit int, cursor string) ([]*E, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, nil, err
		}

		query = query.Start(c)
	}

	return q.Query[E](ctx, r.client, query)
}

func (r *repo[E]) ListTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, cursor string) ([]*E, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, nil, err
		}

		query = query.Start(c)
	}

	return q.QueryTxn[E](ctx, txn, r.client, query)
}

func (r *repo[E]) ListPage(ctx context.Context, ancestor *datastore.Key, limit int, offset int) ([]*E, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if offset > 0 {
		query = query.Offset(offset)
	}

	return q.Query[E](ctx, r.client, query)
}

func (r *repo[E]) ListPageTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, offset int) ([]*E, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if offset > 0 {
		query = query.Offset(offset)
	}

	return q.QueryTxn[E](ctx, txn, r.client, query)
}

func (r *repo[E]) ListKeys(ctx context.Context, ancestor *datastore.Key, limit int, cursor string) ([]*datastore.Key, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, nil, err
		}

		query = query.Start(c)
	}

	return q.QueryKeys(ctx, r.client, query)
}

func (r *repo[E]) ListKeysTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, cursor string) ([]*datastore.Key, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, nil, err
		}

		query = query.Start(c)
	}

	return q.QueryKeysTxn(ctx, txn, r.client, query)
}

func (r *repo[E]) ListAll(ctx context.Context, ancestor *datastore.Key) ([]*E, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := q.Query[E](ctx, r.client, query)

	return e, err
}

func (r *repo[E]) ListAllTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key) ([]*E, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := q.QueryTxn[E](ctx, txn, r.client, query)

	return e, err
}

func (r *repo[E]) ListAllKeys(ctx context.Context, ancestor *datastore.Key) ([]*datastore.Key, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	keys, _, err := q.QueryKeys(ctx, r.client, query)

	return keys, err
}

func (r *repo[E]) ListAllKeysTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key) ([]*datastore.Key, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	keys, _, err := q.QueryKeysTxn(ctx, txn, r.client, query)

	return keys, err
}

func (r *repo[E]) ListProjection(ctx context.Context, ancestor *datastore.Key, limit int, cursor string, generate q.Generator[any], fields ...string) ([]*any, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if cursor != "" {
		c, err := datastore.DecodeCursor(cursor)
		if err != nil {
			return nil, err
		}

		query = query.Start(c)
	}

	e, _, err := q.Project(ctx, r.client, query, generate, fields...)

	return e, err
}

func (r *repo[E]) ListProjectionTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, generate q.Generator[any], fields ...string) ([]*any, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := q.ProjectTxn(ctx, txn, r.client, query, generate, fields...)

	return e, err
}

func (r *repo[E]) ListPageProjection(ctx context.Context, ancestor *datastore.Key, limit int, offset int, generate q.Generator[any], fields ...string) ([]*any, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if offset > 0 {
		query = query.Offset(offset)
	}

	e, c, err := q.Project(ctx, r.client, query, generate, fields...)

	return e, c, err
}

func (r *repo[E]) ListPageProjectionTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, limit int, offset int, generate q.Generator[any], fields ...string) ([]*any, *datastore.Cursor, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if offset > 0 {
		query = query.Offset(offset)
	}

	e, c, err := q.ProjectTxn(ctx, txn, r.client, query, generate, fields...)

	return e, c, err
}

func (r *repo[E]) ListAllProjection(ctx context.Context, ancestor *datastore.Key, generate q.Generator[any], fields ...string) ([]*any, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := q.Project(ctx, r.client, query, generate, fields...)

	return e, err
}

func (r *repo[E]) ListAllProjectionTxn(ctx context.Context, txn q.Transaction, ancestor *datastore.Key, generate q.Generator[any], fields ...string) ([]*any, error) {
	query := datastore.NewQuery(r.kind)

	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	e, _, err := q.ProjectTxn(ctx, txn, r.client, query, generate, fields...)

	return e, err
}

func (r *repo[E]) Update(ctx context.Context, key *datastore.Key, entity *E) error {
	_, err := q.Update(ctx, r.client, key, entity)

	return err
}

func (r *repo[E]) UpdateTxn(txn q.Transaction, key *datastore.Key, entity *E) error {
	_, err := q.UpdateTxn(txn, key, entity)

	return err
}

func (r *repo[E]) UpdateMulti(ctx context.Context, keys []*datastore.Key, entities []*E) error {
	if len(entities) == 0 {
		return nil
	}

	_, err := q.UpdateMulti(ctx, r.client, keys, entities)

	return err
}

func (r *repo[E]) UpdateMultiTxn(txn q.Transaction, keys []*datastore.Key, entities []*E) error {
	if len(entities) == 0 {
		return nil
	}

	_, err := q.UpdateMultiTxn(txn, keys, entities)

	return err
}

func (r *repo[E]) Delete(ctx context.Context, key *datastore.Key) error {
	return q.Delete(ctx, r.client, key)
}

func (r *repo[E]) DeleteTxn(txn q.Transaction, key *datastore.Key) error {
	return q.DeleteTxn(txn, key)
}

func (r *repo[E]) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	return q.DeleteMulti(ctx, r.client, keys)
}

func (r *repo[E]) DeleteMultiTxn(txn q.Transaction, keys []*datastore.Key) error {
	return q.DeleteMultiTxn(txn, keys)
}
