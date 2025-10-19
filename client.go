package dskit

import (
	"context"
	"errors"
	"os"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
)

type Client interface {
	Client() *datastore.Client
	RunInTransaction(ctx context.Context, f func(txn Transaction) error, opts ...datastore.TransactionOption) (*datastore.Commit, error)
}

type client struct {
	client *datastore.Client
}

func (c *client) Client() *datastore.Client {
	return c.client
}

func (c *client) RunInTransaction(ctx context.Context, f func(txn Transaction) error, opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	tx, err := c.client.NewTransaction(ctx, opts...)
	if err != nil {
		return nil, err
	}

	err = f(NewTransaction(tx))
	if err != nil {
		rErr := tx.Rollback()
		if rErr != nil {
			return nil, errors.Join(err, rErr)
		}
		return nil, err
	}

	return tx.Commit()
}

func NewClient(ctx context.Context, databaseID string, options ...option.ClientOption) (Client, error) {
	var opts []option.ClientOption
	var projectID string
	var err error

	opts = append(opts, options...)

	if os.Getenv("DATASTORE_EMULATOR_HOST") == "" {
		if os.Getenv("GCP_PROJECT_ID") != "" {
			projectID = os.Getenv("GCP_PROJECT_ID")
		} else {
			projectID, err = metadata.ProjectIDWithContext(ctx)
			if err != nil {
				return nil, err
			}
		}

		if credentialsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); credentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(credentialsFile))
		}
	} else {
		projectID = os.Getenv("GCP_PROJECT_ID")

		if projectID == "" {
			projectID = os.Getenv("DATASTORE_PROJECT_ID")
		}

		if projectID == "" {
			projectID = databaseID
		}

		if projectID == "" {
			return nil, errors.New("project ID must be provided via GCP_PROJECT_ID, DATASTORE_PROJECT_ID, or databaseID when using the Datastore emulator")
		}
	}

	var c *datastore.Client

	if databaseID != "" {
		c, err = datastore.NewClientWithDatabase(ctx, projectID, databaseID, opts...)
	} else {
		c, err = datastore.NewClient(ctx, projectID, opts...)
	}

	return &client{client: c}, err
}
