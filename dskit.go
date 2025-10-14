package dskit

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

type DSKit interface {
	Client() *datastore.Client
	NewTxn(ctx context.Context) (*datastore.Transaction, error)
	Create(ctx context.Context, key *datastore.Key, entity any) (*datastore.Key, error)
	CreateTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key, entity any) (*datastore.PendingKey, error)
	CreateMulti(ctx context.Context, keys []*datastore.Key, entities []any) ([]*datastore.Key, error)
	CreateMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key, entities []any) ([]*datastore.PendingKey, error)
	Read(ctx context.Context, key *datastore.Key) (any, error)
	ReadTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key) (any, error)
	ReadMulti(ctx context.Context, keys []*datastore.Key) ([]any, error)
	ReadMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key) ([]any, error)
	List(ctx context.Context, kind string, ancestor *datastore.Key) ([]any, *datastore.Cursor, error)
	ListAll(ctx context.Context, kind string, ancestor *datastore.Key) ([]any, error)
	Query(ctx context.Context, kind string, query *datastore.Query) ([]any, *datastore.Cursor, error)
	QueryTxn(ctx context.Context, txn *datastore.Transaction, kind string, query *datastore.Query) ([]any, *datastore.Cursor, error)
	Update(ctx context.Context, key *datastore.Key, entity any) error
	UpdateTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key, entity any) error
	UpdateMulti(ctx context.Context, keys []*datastore.Key, entities []any) error
	UpdateMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key, entities []any) error
	Delete(ctx context.Context, key *datastore.Key) error
	DeleteTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key) error
	DeleteMulti(ctx context.Context, keys []*datastore.Key) error
	DeleteMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key) error
	Exists(ctx context.Context, key *datastore.Key) (bool, error)
	ExistsTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key) (bool, error)
	ExistsForQuery(ctx context.Context, query *datastore.Query) (bool, error)
	ExistsForQueryTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query) (bool, error)
	Count(ctx context.Context, kind string, ancestor *datastore.Key) (int, error)
	CountTxn(ctx context.Context, txn *datastore.Transaction, kind string, ancestor *datastore.Key) (int, error)
	CountForQuery(ctx context.Context, query *datastore.Query) (int, error)
	CountForQueryTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query) (int, error)
	SumField(ctx context.Context, query *datastore.Query, field string) (float64, error)
	SumFieldTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, error)
	SumFieldWithCount(ctx context.Context, query *datastore.Query, field string) (float64, int64, error)
	SumFieldWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, int64, error)
	SumFields(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, error)
	SumFieldsTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, error)
	SumFieldsWithCount(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, int64, error)
	SumFieldsWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, int64, error)
	AvgField(ctx context.Context, query *datastore.Query, field string) (float64, error)
	AvgFieldTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, error)
	AvgFieldWithCount(ctx context.Context, query *datastore.Query, field string) (float64, int64, error)
	AvgFieldWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, int64, error)
	AvgFields(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, error)
	AvgFieldsTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, error)
	AvgFieldsWithCount(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, int64, error)
	AvgFieldsWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, int64, error)
	QueryAggregations(ctx context.Context, query *datastore.Query, sumFields, avgFields []string) (map[string]float64, map[string]float64, error)
	QueryAggregationsTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, sumFields, avgFields []string) (map[string]float64, map[string]float64, error)
	QueryAggregationsWithCount(ctx context.Context, query *datastore.Query, sumFields, avgFields []string) (int64, map[string]float64, map[string]float64, error)
	QueryAggregationsWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, sumFields, avgFields []string) (int64, map[string]float64, map[string]float64, error)
}

var globalKindTypes sync.Map

type dskit struct {
	client *datastore.Client
}

func NewDSKit(client *datastore.Client) DSKit {
	return &dskit{client: client}
}

func (d *dskit) Client() *datastore.Client {
	return d.client
}

func (d *dskit) NewTxn(ctx context.Context) (*datastore.Transaction, error) {
	return d.client.NewTransaction(ctx)
}

func (d *dskit) Create(ctx context.Context, key *datastore.Key, entity any) (*datastore.Key, error) {
	if key == nil {
		newKey, err := normalizeAnyEntityNewKey(entity)
		if err != nil {
			return nil, err
		}

		key = newKey
	} else {
		if _, err := requireAnyEntity(entity); err != nil {
			return nil, err
		}
	}

	d.registerEntityType(entity)
	touchEntityForCreate(entity)

	return d.client.Put(ctx, key, entity)
}

func (d *dskit) CreateTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key, entity any) (*datastore.PendingKey, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	if key == nil {
		newKey, err := normalizeAnyEntityNewKey(entity)
		if err != nil {
			return nil, err
		}

		key = newKey
	} else {
		if _, err := requireAnyEntity(entity); err != nil {
			return nil, err
		}
	}

	d.registerEntityType(entity)
	touchEntityForCreate(entity)

	return txn.Put(key, entity)
}

func (d *dskit) CreateMulti(ctx context.Context, keys []*datastore.Key, entities []any) ([]*datastore.Key, error) {
	normalized, raw, err := normalizeAnyEntitySlice(entities)
	if err != nil {
		return nil, err
	}

	if len(normalized) == 0 {
		return make([]*datastore.Key, 0), nil
	}

	if len(keys) == 0 {
		keys = make([]*datastore.Key, len(normalized))
	} else if len(keys) != len(normalized) {
		return nil, errors.New("keys and entities must have the same length")
	}

	for i := range normalized {
		if keys[i] == nil {
			keys[i] = normalized[i].NewKey()
		}

		d.registerEntityType(raw[i])
		touchEntityForCreate(raw[i])
	}

	return d.client.PutMulti(ctx, keys, raw)
}

func (d *dskit) CreateMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key, entities []any) ([]*datastore.PendingKey, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	normalized, raw, err := normalizeAnyEntitySlice(entities)
	if err != nil {
		return nil, err
	}

	if len(normalized) == 0 {
		return make([]*datastore.PendingKey, 0), nil
	}

	if len(keys) == 0 {
		keys = make([]*datastore.Key, len(normalized))
	} else if len(keys) != len(normalized) {
		return nil, errors.New("keys and entities must have the same length")
	}

	for i := range normalized {
		if keys[i] == nil {
			keys[i] = normalized[i].NewKey()
		}

		d.registerEntityType(raw[i])
		touchEntityForCreate(raw[i])
	}

	return txn.PutMulti(keys, raw)
}

func (d *dskit) Read(ctx context.Context, key *datastore.Key) (any, error) {
	if key == nil {
		return nil, errors.New("key cannot be nil")
	}

	entity, err := d.newEntityForKind(key.Kind)
	if err != nil {
		return nil, err
	}

	err = d.client.Get(ctx, key, entity)
	if errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, nil
	}

	return entity, err
}

func (d *dskit) ReadTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key) (any, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	if key == nil {
		return nil, errors.New("key cannot be nil")
	}

	entity, err := d.newEntityForKind(key.Kind)
	if err != nil {
		return nil, err
	}

	err = txn.Get(key, entity)
	if errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, nil
	}

	return entity, err
}

func (d *dskit) ReadMulti(ctx context.Context, keys []*datastore.Key) ([]any, error) {
	if len(keys) == 0 {
		return make([]any, 0), nil
	}

	entities := make([]any, len(keys))
	for i, key := range keys {
		if key == nil {
			return nil, fmt.Errorf("key at index %d cannot be nil", i)
		}

		entity, err := d.newEntityForKind(key.Kind)
		if err != nil {
			return nil, err
		}

		entities[i] = entity
	}

	dsts, err := normalizePointerSlice(entities)
	if err != nil {
		return nil, err
	}

	if err := d.client.GetMulti(ctx, keys, dsts); err != nil {
		return nil, err
	}

	return entities, nil
}

func (d *dskit) ReadMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key) ([]any, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	if len(keys) == 0 {
		return make([]any, 0), nil
	}

	entities := make([]any, len(keys))
	for i, key := range keys {
		if key == nil {
			return nil, fmt.Errorf("key at index %d cannot be nil", i)
		}

		entity, err := d.newEntityForKind(key.Kind)
		if err != nil {
			return nil, err
		}

		entities[i] = entity
	}

	dsts, err := normalizePointerSlice(entities)
	if err != nil {
		return nil, err
	}

	if err := txn.GetMulti(keys, dsts); err != nil {
		return nil, err
	}

	return entities, nil
}

func (d *dskit) List(ctx context.Context, kind string, ancestor *datastore.Key) ([]any, *datastore.Cursor, error) {
	if kind == "" {
		return nil, nil, errors.New("kind cannot be empty")
	}

	query := datastore.NewQuery(kind)
	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	return d.runQuery(ctx, kind, query)
}

func (d *dskit) ListAll(ctx context.Context, kind string, ancestor *datastore.Key) ([]any, error) {
	if kind == "" {
		return nil, errors.New("kind cannot be empty")
	}

	query := datastore.NewQuery(kind)
	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	results, _, err := d.runQuery(ctx, kind, query)
	return results, err
}

func (d *dskit) Query(ctx context.Context, kind string, query *datastore.Query) ([]any, *datastore.Cursor, error) {
	if kind == "" {
		return nil, nil, errors.New("kind cannot be empty")
	}

	if query == nil {
		query = datastore.NewQuery(kind)
	}

	return d.runQuery(ctx, kind, query)
}

func (d *dskit) QueryTxn(ctx context.Context, txn *datastore.Transaction, kind string, query *datastore.Query) ([]any, *datastore.Cursor, error) {
	if txn == nil {
		return nil, nil, errors.New("transaction cannot be nil")
	}

	if kind == "" {
		return nil, nil, errors.New("kind cannot be empty")
	}

	if query == nil {
		query = datastore.NewQuery(kind)
	}

	query = query.Transaction(txn)

	return d.runQuery(ctx, kind, query)
}

func (d *dskit) Update(ctx context.Context, key *datastore.Key, entity any) error {
	if key == nil {
		resolvedKey, err := normalizeAnyEntityKey(entity)
		if err != nil {
			return err
		}

		key = resolvedKey
	} else {
		if _, err := requireAnyEntity(entity); err != nil {
			return err
		}
	}

	touchEntityForUpdate(entity)
	d.registerEntityType(entity)

	_, err := d.client.Put(ctx, key, entity)

	return err
}

func (d *dskit) UpdateTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key, entity any) error {
	if txn == nil {
		return errors.New("transaction cannot be nil")
	}

	if key == nil {
		resolvedKey, err := normalizeAnyEntityKey(entity)
		if err != nil {
			return err
		}

		key = resolvedKey
	} else {
		if _, err := requireAnyEntity(entity); err != nil {
			return err
		}
	}

	touchEntityForUpdate(entity)
	d.registerEntityType(entity)

	_, err := txn.Put(key, entity)

	return err
}

func (d *dskit) UpdateMulti(ctx context.Context, keys []*datastore.Key, entities []any) error {
	normalized, raw, err := normalizeAnyEntitySlice(entities)
	if err != nil {
		return err
	}

	if len(normalized) == 0 {
		return nil
	}

	if len(keys) == 0 {
		keys = make([]*datastore.Key, len(normalized))
	} else if len(keys) != len(normalized) {
		return errors.New("keys and entities must have the same length")
	}

	for i := range normalized {
		if keys[i] == nil {
			keys[i] = normalized[i].Key()
		}

		d.registerEntityType(raw[i])
		touchEntityForUpdate(raw[i])
	}

	_, err = d.client.PutMulti(ctx, keys, raw)

	return err
}

func (d *dskit) UpdateMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key, entities []any) error {
	if txn == nil {
		return errors.New("transaction cannot be nil")
	}

	normalized, raw, err := normalizeAnyEntitySlice(entities)
	if err != nil {
		return err
	}

	if len(normalized) == 0 {
		return nil
	}

	if len(keys) == 0 {
		keys = make([]*datastore.Key, len(normalized))
	} else if len(keys) != len(normalized) {
		return errors.New("keys and entities must have the same length")
	}

	for i := range normalized {
		if keys[i] == nil {
			keys[i] = normalized[i].Key()
		}

		d.registerEntityType(raw[i])
		touchEntityForUpdate(raw[i])
	}

	_, err = txn.PutMulti(keys, raw)

	return err
}

func (d *dskit) Delete(ctx context.Context, key *datastore.Key) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}

	return d.client.Delete(ctx, key)
}

func (d *dskit) DeleteTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key) error {
	if txn == nil {
		return errors.New("transaction cannot be nil")
	}

	if key == nil {
		return errors.New("key cannot be nil")
	}

	return txn.Delete(key)
}

func (d *dskit) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	if len(keys) == 0 {
		return nil
	}

	for i, key := range keys {
		if key == nil {
			return fmt.Errorf("key at index %d cannot be nil", i)
		}
	}

	return d.client.DeleteMulti(ctx, keys)
}

func (d *dskit) DeleteMultiTxn(ctx context.Context, txn *datastore.Transaction, keys []*datastore.Key) error {
	if txn == nil {
		return errors.New("transaction cannot be nil")
	}

	if len(keys) == 0 {
		return nil
	}

	for i, key := range keys {
		if key == nil {
			return fmt.Errorf("key at index %d cannot be nil", i)
		}
	}

	return txn.DeleteMulti(keys)
}

func (d *dskit) Exists(ctx context.Context, key *datastore.Key) (bool, error) {
	return Exists(ctx, d.client, key)
}

func (d *dskit) ExistsTxn(ctx context.Context, txn *datastore.Transaction, key *datastore.Key) (bool, error) {
	return ExistsTxn(ctx, txn, d.client, key)
}

func (d *dskit) ExistsForQuery(ctx context.Context, query *datastore.Query) (bool, error) {
	return ExistsForQuery(ctx, d.client, query)
}

func (d *dskit) ExistsForQueryTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query) (bool, error) {
	return ExistsForQueryTxn(ctx, txn, d.client, query)
}

func (d *dskit) Count(ctx context.Context, kind string, ancestor *datastore.Key) (int, error) {
	if kind == "" {
		return 0, errors.New("kind cannot be empty")
	}

	query := datastore.NewQuery(kind)
	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	count, err := CountForQuery(ctx, d.client, query)
	return int(count), err
}

func (d *dskit) CountTxn(ctx context.Context, txn *datastore.Transaction, kind string, ancestor *datastore.Key) (int, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if kind == "" {
		return 0, errors.New("kind cannot be empty")
	}

	query := datastore.NewQuery(kind)
	if ancestor != nil {
		query = query.Ancestor(ancestor)
	}

	count, err := CountForQueryTxn(ctx, txn, d.client, query)
	return int(count), err
}

func (d *dskit) CountForQuery(ctx context.Context, query *datastore.Query) (int, error) {
	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	count, err := CountForQuery(ctx, d.client, query)
	return int(count), err
}

func (d *dskit) CountForQueryTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query) (int, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	count, err := CountForQueryTxn(ctx, txn, d.client, query)
	return int(count), err
}

func (d *dskit) SumField(ctx context.Context, query *datastore.Query, field string) (float64, error) {
	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	sum, err := SumForField(ctx, d.client, query, field)
	return sum, err
}

func (d *dskit) SumFieldTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	sum, err := SumForFieldTxn(ctx, txn, d.client, query, field)
	return sum, err
}

func (d *dskit) SumFieldWithCount(ctx context.Context, query *datastore.Query, field string) (float64, int64, error) {
	if query == nil {
		return 0, 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, 0, errors.New("field cannot be empty")
	}

	count, sums, _, err := QueryAggregationsWithCount(ctx, d.client, query, []string{field}, nil)
	if err != nil {
		return 0, 0, err
	}

	return sums[field], count, nil
}

func (d *dskit) SumFieldWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, int64, error) {
	if txn == nil {
		return 0, 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, 0, errors.New("field cannot be empty")
	}

	count, sums, _, err := QueryAggregationsWithCountTxn(ctx, txn, d.client, query, []string{field}, nil)
	if err != nil {
		return 0, 0, err
	}

	return sums[field], count, nil
}

func (d *dskit) SumFields(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	sums, _, err := QueryAggregations(ctx, d.client, query, fields, nil)
	if err != nil {
		return nil, err
	}

	return sums, nil
}

func (d *dskit) SumFieldsTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	sums, _, err := QueryAggregationsTxn(ctx, txn, d.client, query, fields, nil)
	if err != nil {
		return nil, err
	}

	return sums, nil
}

func (d *dskit) SumFieldsWithCount(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, int64, error) {
	if query == nil {
		return nil, 0, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, 0, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	count, sums, _, err := QueryAggregationsWithCount(ctx, d.client, query, fields, nil)
	if err != nil {
		return nil, 0, err
	}

	return sums, count, nil
}

func (d *dskit) SumFieldsWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, int64, error) {
	if txn == nil {
		return nil, 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, 0, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, 0, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	count, sums, _, err := QueryAggregationsWithCountTxn(ctx, txn, d.client, query, fields, nil)
	if err != nil {
		return nil, 0, err
	}

	return sums, count, nil
}

func (d *dskit) AvgField(ctx context.Context, query *datastore.Query, field string) (float64, error) {
	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	return AverageForField(ctx, d.client, query, field)
}

func (d *dskit) AvgFieldTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	return AverageForFieldTxn(ctx, txn, d.client, query, field)
}

func (d *dskit) AvgFieldWithCount(ctx context.Context, query *datastore.Query, field string) (float64, int64, error) {
	if query == nil {
		return 0, 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, 0, errors.New("field cannot be empty")
	}

	count, _, avgs, err := QueryAggregationsWithCount(ctx, d.client, query, nil, []string{field})
	if err != nil {
		return 0, 0, err
	}

	return avgs[field], count, nil
}

func (d *dskit) AvgFieldWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, field string) (float64, int64, error) {
	if txn == nil {
		return 0, 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, 0, errors.New("field cannot be empty")
	}

	count, _, avgs, err := QueryAggregationsWithCountTxn(ctx, txn, d.client, query, nil, []string{field})
	if err != nil {
		return 0, 0, err
	}

	return avgs[field], count, nil
}

func (d *dskit) AvgFields(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	_, avgs, err := QueryAggregations(ctx, d.client, query, nil, fields)
	if err != nil {
		return nil, err
	}

	return avgs, nil
}

func (d *dskit) AvgFieldsTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	_, avgs, err := QueryAggregationsTxn(ctx, txn, d.client, query, nil, fields)
	if err != nil {
		return nil, err
	}

	return avgs, nil
}

func (d *dskit) AvgFieldsWithCount(ctx context.Context, query *datastore.Query, fields ...string) (map[string]float64, int64, error) {
	if query == nil {
		return nil, 0, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, 0, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	count, _, avgs, err := QueryAggregationsWithCount(ctx, d.client, query, nil, fields)
	if err != nil {
		return nil, 0, err
	}

	return avgs, count, nil
}

func (d *dskit) AvgFieldsWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, fields ...string) (map[string]float64, int64, error) {
	if txn == nil {
		return nil, 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, 0, errors.New("query cannot be nil")
	}

	for i, field := range fields {
		if field == "" {
			return nil, 0, fmt.Errorf("fields[%d] cannot be empty", i)
		}
	}

	count, _, avgs, err := QueryAggregationsWithCountTxn(ctx, txn, d.client, query, nil, fields)
	if err != nil {
		return nil, 0, err
	}

	return avgs, count, nil
}

func (d *dskit) QueryAggregations(ctx context.Context, query *datastore.Query, sumFields, avgFields []string) (map[string]float64, map[string]float64, error) {
	return QueryAggregations(ctx, d.client, query, sumFields, avgFields)
}

func (d *dskit) QueryAggregationsTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, sumFields, avgFields []string) (map[string]float64, map[string]float64, error) {
	if txn == nil {
		return nil, nil, errors.New("transaction cannot be nil")
	}

	return QueryAggregationsTxn(ctx, txn, d.client, query, sumFields, avgFields)
}

func (d *dskit) QueryAggregationsWithCount(ctx context.Context, query *datastore.Query, sumFields, avgFields []string) (int64, map[string]float64, map[string]float64, error) {
	return QueryAggregationsWithCount(ctx, d.client, query, sumFields, avgFields)
}

func (d *dskit) QueryAggregationsWithCountTxn(ctx context.Context, txn *datastore.Transaction, query *datastore.Query, sumFields, avgFields []string) (int64, map[string]float64, map[string]float64, error) {
	if txn == nil {
		return 0, nil, nil, errors.New("transaction cannot be nil")
	}

	return QueryAggregationsWithCountTxn(ctx, txn, d.client, query, sumFields, avgFields)
}

func (d *dskit) runQuery(ctx context.Context, kind string, query *datastore.Query) ([]any, *datastore.Cursor, error) {
	typ, err := d.kindType(kind)
	if err != nil {
		return nil, nil, err
	}

	it := d.client.Run(ctx, query)
	results := make([]any, 0, defaultQueryCapacity)

	for {
		entity := reflect.New(typ.Elem()).Interface()

		_, err := it.Next(entity)
		if errors.Is(err, iterator.Done) {
			break
		}

		var errFieldMismatch *datastore.ErrFieldMismatch
		if errors.As(err, &errFieldMismatch) {
			results = append(results, entity)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		results = append(results, entity)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return results, &c, nil
}

func (d *dskit) registerEntityType(entity any) {
	if entity == nil {
		return
	}

	ent, ok := entity.(Entity)
	if !ok {
		return
	}

	kind := ent.Kind()
	if kind == "" {
		return
	}

	typ := reflect.TypeOf(entity)
	if typ == nil {
		return
	}

	if typ.Kind() != reflect.Pointer {
		typ = reflect.PointerTo(typ)
	}

	rememberKindType(kind, typ)
}

func (d *dskit) kindType(kind string) (reflect.Type, error) {
	if kind == "" {
		return nil, errors.New("kind cannot be empty")
	}

	value, ok := globalKindTypes.Load(kind)
	if !ok {
		return nil, fmt.Errorf("entity type for kind %q not registered", kind)
	}

	typ, ok := value.(reflect.Type)
	if !ok {
		return nil, errors.New("invalid entity type registry entry")
	}

	if typ.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("entity type for kind %q must be a pointer", kind)
	}

	return typ, nil
}

func rememberKindType(kind string, typ reflect.Type) {
	if kind == "" || typ == nil {
		return
	}

	if typ.Kind() != reflect.Pointer {
		typ = reflect.PointerTo(typ)
	}

	globalKindTypes.Store(kind, typ)
}

func (d *dskit) newEntityForKind(kind string) (any, error) {
	typ, err := d.kindType(kind)
	if err != nil {
		return nil, err
	}

	return reflect.New(typ.Elem()).Interface(), nil
}

func requireAnyEntity(entity any) (Entity, error) {
	if entity == nil {
		return nil, errors.New("entity cannot be nil")
	}

	ent, ok := entity.(Entity)
	if !ok {
		return nil, fmt.Errorf("entity of type %T does not implement dskit.Entity", entity)
	}

	return ent, nil
}

func normalizeAnyEntityNewKey(entity any) (*datastore.Key, error) {
	ent, err := requireAnyEntity(entity)
	if err != nil {
		return nil, err
	}

	return ent.NewKey(), nil
}

func normalizeAnyEntityKey(entity any) (*datastore.Key, error) {
	ent, err := requireAnyEntity(entity)
	if err != nil {
		return nil, err
	}

	return ent.Key(), nil
}

func normalizeAnyEntitySlice(entities []any) ([]Entity, []any, error) {
	normalized := make([]Entity, len(entities))
	raw := make([]any, len(entities))

	for i, entity := range entities {
		ent, err := requireAnyEntity(entity)
		if err != nil {
			return nil, nil, fmt.Errorf("entity at index %d: %w", i, err)
		}

		normalized[i] = ent
		raw[i] = entity
	}

	return normalized, raw, nil
}

func requirePointer(value any) error {
	if value == nil {
		return errors.New("destination cannot be nil")
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer, got %T", value)
	}

	return nil
}

func normalizePointerSlice(values []any) ([]any, error) {
	result := make([]any, len(values))
	for i, value := range values {
		if err := requirePointer(value); err != nil {
			return nil, fmt.Errorf("destination at index %d: %w", i, err)
		}

		result[i] = value
	}

	return result, nil
}
