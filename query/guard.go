package query

import (
	"errors"
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
)

func requiresClient(client Client) error {
	if client == nil {
		return errors.New("client cannot be nil")
	}

	if client.Client() == nil {
		return errors.New("client datastore cannot be nil")
	}

	return nil
}

func requiresKey(key *datastore.Key) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}

	return nil
}

func requiresKeys(keys []*datastore.Key) error {
	if keys == nil {
		return errors.New("keys cannot be nil")
	}

	for _, k := range keys {
		if err := requiresKey(k); err != nil {
			return err
		}
	}

	return nil
}

func requiresEntity(entity any) error {
	if entity == nil {
		return errors.New("entity is nil")
	}

	v := reflect.ValueOf(entity)

	if v.Kind() != reflect.Pointer {
		return fmt.Errorf("entity must be a pointer to a struct, got %T", entity)
	}

	if v.IsNil() {
		return errors.New("entity pointer is nil")
	}

	if v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("entity must point to a struct, got pointer to %s", v.Elem().Kind())
	}

	return nil
}

func requiresEntities[E any](entities []*E) error {
	if entities == nil {
		return errors.New("entities cannot be nil")
	}

	for _, e := range entities {
		if err := requiresEntity(e); err != nil {
			return err
		}
	}

	return nil
}

func requiresEqualLength[E any](keys []*datastore.Key, entities []*E) error {
	if len(keys) != len(entities) {
		return errors.New("keys and entities must have the same length")
	}

	return nil
}

func requiresTransaction(txn Transaction) error {
	if txn == nil {
		return errors.New("transaction cannot be nil")
	}

	if txn.Txn() == nil {
		return errors.New("transaction datastore cannot be nil")
	}

	return nil
}

func requiresQuery(query *datastore.Query) error {
	if query == nil {
		return errors.New("query cannot be nil")
	}

	return nil
}

func requiresField(field string) error {
	if field == "" {
		return errors.New("field cannot be empty")
	}

	return nil
}

func requiresFields(fields []string) error {
	if fields == nil {
		return errors.New("fields cannot be nil")
	}

	for _, f := range fields {
		if err := requiresField(f); err != nil {
			return err
		}
	}

	return nil
}

func requiresOneField(fields ...[]string) error {
	if len(fields) == 0 {
		return errors.New("at least one field slice must be provided")
	}

	found := false

	for _, fs := range fields {
		if len(fs) > 0 {
			found = true

			break
		}
	}

	if !found {
		return errors.New("at least one field must be provided")
	}

	return nil
}
