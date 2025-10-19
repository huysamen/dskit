package query

import (
	"context"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

func CountForQuery(ctx context.Context, client Client, query *datastore.Query) (int64, error) {
	if err := requiresClient(client); err != nil {
		return 0, err
	}

	if err := requiresQuery(query); err != nil {
		return 0, err
	}

	r, err := client.Client().RunAggregationQuery(ctx, query.NewAggregationQuery().WithCount("count"))
	if err != nil {
		return 0, err
	}

	c := r["count"]
	v := c.(*datastorepb.Value)

	return v.GetIntegerValue(), nil
}

func CountForQueryTxn(ctx context.Context, txn Transaction, client Client, query *datastore.Query) (int64, error) {
	if err := requiresTransaction(txn); err != nil {
		return 0, err
	}

	if err := requiresClient(client); err != nil {
		return 0, err
	}

	if err := requiresQuery(query); err != nil {
		return 0, err
	}

	r, err := client.Client().RunAggregationQuery(ctx, query.Transaction(txn.Txn()).NewAggregationQuery().WithCount("count"))
	if err != nil {
		return 0, err
	}

	c := r["count"]
	v := c.(*datastorepb.Value)

	return v.GetIntegerValue(), nil
}
