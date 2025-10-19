package query

import (
	"context"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

func SumForField(ctx context.Context, client Client, query *datastore.Query, field string) (float64, error) {
	s, err := SumForFields(ctx, client, query, field)
	if err != nil {
		return 0, err
	}

	return s[field], nil
}

func SumForFieldTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query, field string) (float64, error) {
	s, err := SumForFieldsTxn(ctx, txn, client, query, field)
	if err != nil {
		return 0, err
	}

	return s[field], nil
}

func SumForFields(ctx context.Context, client Client, query *datastore.Query, fields ...string) (map[string]float64, error) {
	if err := requiresClient(client); err != nil {
		return nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, err
	}

	if err := requiresFields(fields); err != nil {
		return nil, err
	}

	aq := query.NewAggregationQuery()
	sums := make(map[string]float64)

	for _, f := range fields {
		aq = aq.WithSum(f, f)
	}

	r, err := client.Client().RunAggregationQuery(ctx, aq)
	if err != nil {
		return nil, err
	}

	for _, f := range fields {
		sv := r[f]
		v := sv.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return nil, err
		}

		sums[f] = value
	}

	return sums, nil
}

func SumForFieldsTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query, fields ...string) (map[string]float64, error) {
	if err := requiresTransaction(txn); err != nil {
		return nil, err
	}

	if err := requiresClient(client); err != nil {
		return nil, err
	}

	if err := requiresQuery(query); err != nil {
		return nil, err
	}

	if err := requiresFields(fields); err != nil {
		return nil, err
	}

	aq := query.Transaction(txn.Txn()).NewAggregationQuery()
	sums := make(map[string]float64)

	for _, f := range fields {
		aq = aq.WithSum(f, f)
	}

	r, err := client.Client().RunAggregationQuery(ctx, aq)
	if err != nil {
		return nil, err
	}

	for _, f := range fields {
		sv := r[f]
		v := sv.(*datastorepb.Value)

		value, err := numericValue(v)
		if err != nil {
			return nil, err
		}

		sums[f] = value
	}

	return sums, nil
}
