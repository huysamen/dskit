package dskit

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/api/iterator"
)

const defaultQueryCapacity = 16

func Exists(ctx context.Context, client *datastore.Client, key *datastore.Key) (bool, error) {
	if key == nil {
		return false, errors.New("key cannot be nil")
	}

	return ExistsForQuery(
		ctx,
		client,
		datastore.NewQuery(key.Kind).FilterField("__key__", "=", key),
	)
}

func ExistsTxn(ctx context.Context, txn *datastore.Transaction, client *datastore.Client, key *datastore.Key) (bool, error) {
	if txn == nil {
		return false, errors.New("transaction cannot be nil")
	}

	if key == nil {
		return false, errors.New("key cannot be nil")
	}

	return ExistsForQueryTxn(
		ctx,
		txn,
		client,
		datastore.NewQuery(key.Kind).FilterField("__key__", "=", key),
	)
}

func ExistsForQuery(ctx context.Context, client *datastore.Client, query *datastore.Query) (bool, error) {
	if query == nil {
		return false, errors.New("query cannot be nil")
	}

	query = query.KeysOnly().Limit(1)

	count, err := CountForQuery(ctx, client, query)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func ExistsForQueryTxn(ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query) (bool, error) {
	if txn == nil {
		return false, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return false, errors.New("query cannot be nil")
	}

	query = query.KeysOnly().Limit(1).Transaction(txn)

	count, err := CountForQuery(ctx, client, query)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func Query[E Entity](ctx context.Context, client *datastore.Client, query *datastore.Query) ([]*E, *datastore.Cursor, error) {
	if query == nil {
		return nil, nil, errors.New("query cannot be nil")
	}

	it := client.Run(ctx, query)
	entities := make([]*E, 0)

	for {
		var e E

		_, err := it.Next(&e)
		if errors.Is(err, iterator.Done) {
			break
		}

		var errFieldMismatch *datastore.ErrFieldMismatch
		if errors.As(err, &errFieldMismatch) {
			entity := e
			entities = append(entities, &entity)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		entity := e
		entities = append(entities, &entity)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return entities, &c, nil
}

func QueryTxn[E Entity](ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query) ([]*E, *datastore.Cursor, error) {
	if txn == nil {
		return nil, nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, nil, errors.New("query cannot be nil")
	}

	query = query.Transaction(txn)

	it := client.Run(ctx, query)
	entities := make([]*E, 0)

	for {
		var e E

		_, err := it.Next(&e)
		if errors.Is(err, iterator.Done) {
			break
		}

		var errFieldMismatch *datastore.ErrFieldMismatch
		if errors.As(err, &errFieldMismatch) {
			entity := e
			entities = append(entities, &entity)

			continue
		}

		if err != nil {
			return nil, nil, err
		}

		entity := e
		entities = append(entities, &entity)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return entities, &c, nil
}

func QueryOne[E Entity](ctx context.Context, client *datastore.Client, query *datastore.Query) (*E, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	query = query.Limit(1)

	entities, _, err := Query[E](ctx, client, query)
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return nil, nil
	}

	return entities[0], nil
}

func QueryOneTxn[E Entity](ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query) (*E, error) {
	if txn == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	query = query.Limit(1).Transaction(txn)

	entities, _, err := Query[E](ctx, client, query)
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return nil, nil
	}

	return entities[0], nil
}

func QueryKeys(ctx context.Context, client *datastore.Client, query *datastore.Query) ([]*datastore.Key, *datastore.Cursor, error) {
	if query == nil {
		return nil, nil, errors.New("query cannot be nil")
	}

	query = query.KeysOnly()
	it := client.Run(ctx, query)
	keys := make([]*datastore.Key, 0)

	for {
		key, err := it.Next(nil)
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		keys = append(keys, key)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return keys, &c, nil
}

func QueryKeysTxn(ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query) ([]*datastore.Key, *datastore.Cursor, error) {
	if txn == nil {
		return nil, nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, nil, errors.New("query cannot be nil")
	}

	query = query.KeysOnly().Transaction(txn)
	it := client.Run(ctx, query)
	keys := make([]*datastore.Key, 0)

	for {
		key, err := it.Next(nil)
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		keys = append(keys, key)
	}

	c, err := it.Cursor()
	if err != nil {
		return nil, nil, err
	}

	return keys, &c, nil
}

func CountForQuery(ctx context.Context, client *datastore.Client, query *datastore.Query) (int64, error) {
	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	aq := query.NewAggregationQuery().WithCount("count")

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}

	c := r["count"]
	v := c.(*datastorepb.Value)

	return v.GetIntegerValue(), nil
}

func CountForQueryTxn(ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query) (int64, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	aq := query.Transaction(txn).NewAggregationQuery().WithCount("count")

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}

	c := r["count"]
	v := c.(*datastorepb.Value)

	return v.GetIntegerValue(), nil
}

func SumForField(ctx context.Context, client *datastore.Client, query *datastore.Query, field string) (float64, error) {
	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	aq := query.NewAggregationQuery().WithSum("sum", field)

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}

	s := r["sum"]
	v := s.(*datastorepb.Value)

	return numericValue(v)
}

func SumForFieldTxn(ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query, field string) (float64, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	aq := query.Transaction(txn).NewAggregationQuery().WithSum("sum", field)

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}

	s := r["sum"]
	v := s.(*datastorepb.Value)

	return numericValue(v)
}

func AverageForField(ctx context.Context, client *datastore.Client, query *datastore.Query, field string) (float64, error) {
	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	aq := query.NewAggregationQuery().WithAvg("avg", field)

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}

	a := r["avg"]
	v := a.(*datastorepb.Value)

	return v.GetDoubleValue(), nil
}

func AverageForFieldTxn(ctx context.Context, txn *datastore.Transaction, client *datastore.Client, query *datastore.Query, field string) (float64, error) {
	if txn == nil {
		return 0, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, errors.New("query cannot be nil")
	}

	if field == "" {
		return 0, errors.New("field cannot be empty")
	}

	aq := query.Transaction(txn).NewAggregationQuery().WithAvg("avg", field)

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, err
	}

	a := r["avg"]
	v := a.(*datastorepb.Value)

	return v.GetDoubleValue(), nil
}

func QueryAggregations(
	ctx context.Context,
	client *datastore.Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (map[string]float64, map[string]float64, error) {
	if query == nil {
		return nil, nil, errors.New("query cannot be nil")
	}

	for i, field := range sumFields {
		if field == "" {
			return nil, nil, fmt.Errorf("sumFields[%d] cannot be empty", i)
		}
	}

	for i, field := range avgFields {
		if field == "" {
			return nil, nil, fmt.Errorf("avgFields[%d] cannot be empty", i)
		}
	}

	aq := query.NewAggregationQuery()

	sums := make(map[string]float64)
	avgs := make(map[string]float64)

	for _, s := range sumFields {
		aq = aq.WithSum(s, s)
	}

	for _, a := range avgFields {
		aq = aq.WithAvg(a, a)
	}

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return nil, nil, err
	}

	for _, s := range sumFields {
		sv := r[s]
		v := sv.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return nil, nil, fmt.Errorf("sum field %q: %w", s, err)
		}

		sums[s] = value
	}

	for _, a := range avgFields {
		av := r[a]
		v := av.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return nil, nil, fmt.Errorf("avg field %q: %w", a, err)
		}

		avgs[a] = value
	}

	return sums, avgs, nil
}

func QueryAggregationsTxn(
	ctx context.Context,
	txn *datastore.Transaction,
	client *datastore.Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (map[string]float64, map[string]float64, error) {
	if txn == nil {
		return nil, nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return nil, nil, errors.New("query cannot be nil")
	}

	for i, field := range sumFields {
		if field == "" {
			return nil, nil, fmt.Errorf("sumFields[%d] cannot be empty", i)
		}
	}

	for i, field := range avgFields {
		if field == "" {
			return nil, nil, fmt.Errorf("avgFields[%d] cannot be empty", i)
		}
	}

	aq := query.Transaction(txn).NewAggregationQuery()

	sums := make(map[string]float64)
	avgs := make(map[string]float64)

	for _, s := range sumFields {
		aq = aq.WithSum(s, s)
	}

	for _, a := range avgFields {
		aq = aq.WithAvg(a, a)
	}

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return nil, nil, err
	}

	for _, s := range sumFields {
		sv := r[s]
		v := sv.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return nil, nil, fmt.Errorf("sum field %q: %w", s, err)
		}

		sums[s] = value
	}

	for _, a := range avgFields {
		av := r[a]
		v := av.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return nil, nil, fmt.Errorf("avg field %q: %w", a, err)
		}

		avgs[a] = value
	}

	return sums, avgs, nil
}

func QueryAggregationsWithCount(
	ctx context.Context,
	client *datastore.Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (int64, map[string]float64, map[string]float64, error) {
	if query == nil {
		return 0, nil, nil, errors.New("query cannot be nil")
	}

	for i, field := range sumFields {
		if field == "" {
			return 0, nil, nil, fmt.Errorf("sumFields[%d] cannot be empty", i)
		}
	}

	for i, field := range avgFields {
		if field == "" {
			return 0, nil, nil, fmt.Errorf("avgFields[%d] cannot be empty", i)
		}
	}

	aq := query.NewAggregationQuery().WithCount("count")

	sums := make(map[string]float64)
	avgs := make(map[string]float64)

	for _, s := range sumFields {
		aq = aq.WithSum(s, s)
	}

	for _, a := range avgFields {
		aq = aq.WithAvg(a, a)
	}

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, nil, nil, err
	}

	c := r["count"]
	v := c.(*datastorepb.Value)
	count := v.GetIntegerValue()

	for _, s := range sumFields {
		sv := r[s]
		v := sv.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("sum field %q: %w", s, err)
		}

		sums[s] = value
	}

	for _, a := range avgFields {
		av := r[a]
		v := av.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("avg field %q: %w", a, err)
		}

		avgs[a] = value
	}

	return count, sums, avgs, nil
}

func QueryAggregationsWithCountTxn(
	ctx context.Context,
	txn *datastore.Transaction,
	client *datastore.Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (int64, map[string]float64, map[string]float64, error) {
	if txn == nil {
		return 0, nil, nil, errors.New("transaction cannot be nil")
	}

	if query == nil {
		return 0, nil, nil, errors.New("query cannot be nil")
	}

	for i, field := range sumFields {
		if field == "" {
			return 0, nil, nil, fmt.Errorf("sumFields[%d] cannot be empty", i)
		}
	}

	for i, field := range avgFields {
		if field == "" {
			return 0, nil, nil, fmt.Errorf("avgFields[%d] cannot be empty", i)
		}
	}

	aq := query.Transaction(txn).NewAggregationQuery().WithCount("count")

	sums := make(map[string]float64)
	avgs := make(map[string]float64)

	for _, s := range sumFields {
		aq = aq.WithSum(s, s)
	}

	for _, a := range avgFields {
		aq = aq.WithAvg(a, a)
	}

	r, err := client.RunAggregationQuery(ctx, aq)
	if err != nil {
		return 0, nil, nil, err
	}

	c := r["count"]
	v := c.(*datastorepb.Value)
	count := v.GetIntegerValue()

	for _, s := range sumFields {
		sv := r[s]
		v := sv.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("sum field %q: %w", s, err)
		}

		sums[s] = value
	}

	for _, a := range avgFields {
		av := r[a]
		v := av.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("avg field %q: %w", a, err)
		}

		avgs[a] = value
	}

	return count, sums, avgs, nil
}

func numericValue(v *datastorepb.Value) (float64, error) {
	switch value := v.GetValueType().(type) {
	case *datastorepb.Value_IntegerValue:
		return float64(value.IntegerValue), nil
	case *datastorepb.Value_DoubleValue:
		return value.DoubleValue, nil
	default:
		return 0, fmt.Errorf("unexpected value type %T for numeric aggregation", value)
	}
}
