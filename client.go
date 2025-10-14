package dskit

import (
	"context"
	"errors"
	"os"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
)

func NewClient(ctx context.Context, databaseID string, options ...option.ClientOption) (*datastore.Client, error) {
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

	return datastore.NewClient(ctx, projectID, opts...)
}
