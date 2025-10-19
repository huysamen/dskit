package query

import (
	"context"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

func AverageForField(ctx context.Context, client Client, query *datastore.Query, field string) (float64, error) {
	a, err := AverageForFields(ctx, client, query, field)
	if err != nil {
		return 0, err
	}

	return a[field], nil
}

func AverageForFieldTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query, field string) (float64, error) {
	a, err := AverageForFieldsTxn(ctx, txn, client, query, field)
	if err != nil {
		return 0, err
	}

	return a[field], nil
}

func AverageForFields(ctx context.Context, client Client, query *datastore.Query, fields ...string) (map[string]float64, error) {
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
	avgs := make(map[string]float64)

	for _, f := range fields {
		aq = aq.WithAvg(f, f)
	}

	r, err := client.Client().RunAggregationQuery(ctx, aq)
	if err != nil {
		return nil, err
	}

	for _, f := range fields {
		av := r[f]
		v := av.(*datastorepb.Value)

		avgs[f] = v.GetDoubleValue()
	}

	return avgs, nil
}

func AverageForFieldsTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query, fields ...string) (map[string]float64, error) {
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
	avgs := make(map[string]float64)

	for _, f := range fields {
		aq = aq.WithAvg(f, f)
	}

	r, err := client.Client().RunAggregationQuery(ctx, aq)
	if err != nil {
		return nil, err
	}

	for _, f := range fields {
		av := r[f]
		v := av.(*datastorepb.Value)

		avgs[f] = v.GetDoubleValue()
	}

	return avgs, nil
}
