package query

import (
	"context"
	"fmt"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

func QueryAggregations(
	ctx context.Context,
	client Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (map[string]float64, map[string]float64, error) {
	if err := requiresClient(client); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	if err := requiresOneField(sumFields, avgFields); err != nil {
		return nil, nil, err
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

	r, err := client.Client().RunAggregationQuery(ctx, aq)
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
	txn Transaction,
	client Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (map[string]float64, map[string]float64, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, nil, err
	}

	if err := requiresClient(client); err != nil {
		return nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, nil, err
	}

	if err := requiresOneField(sumFields, avgFields); err != nil {
		return nil, nil, err
	}

	aq := query.Transaction(txn.Txn()).NewAggregationQuery()

	sums := make(map[string]float64)
	avgs := make(map[string]float64)

	for _, s := range sumFields {
		aq = aq.WithSum(s, s)
	}

	for _, a := range avgFields {
		aq = aq.WithAvg(a, a)
	}

	r, err := client.Client().RunAggregationQuery(ctx, aq)
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
	client Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (int64, map[string]float64, map[string]float64, error) {
	if err := requiresClient(client); err != nil {
		return 0, nil, nil, err
	}

	if err := requiresQuery(query); err != nil {
		return 0, nil, nil, err
	}

	if err := requiresOneField(sumFields, avgFields); err != nil {
		return 0, nil, nil, err
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

	r, err := client.Client().RunAggregationQuery(ctx, aq)
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
	txn Transaction,
	client Client,
	query *datastore.Query,
	sumFields, avgFields []string,
) (int64, map[string]float64, map[string]float64, error) {
	if err := requiresTransaction(txn); err != nil {
		return 0, nil, nil, err
	}

	if err := requiresClient(client); err != nil {
		return 0, nil, nil, err
	}

	if err := requiresOneField(sumFields, avgFields); err != nil {
		return 0, nil, nil, err
	}

	aq := query.Transaction(txn.Txn()).NewAggregationQuery().WithCount("count")

	sums := make(map[string]float64)
	avgs := make(map[string]float64)

	for _, s := range sumFields {
		aq = aq.WithSum(s, s)
	}

	for _, a := range avgFields {
		aq = aq.WithAvg(a, a)
	}

	r, err := client.Client().RunAggregationQuery(ctx, aq)
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
