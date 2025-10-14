package dskit

import (
	"reflect"
	"time"

	"cloud.google.com/go/datastore"
)

const (
	entityFieldKey     = "__key__"
	entityFieldCreated = "_created"
	entityFieldUpdated = "_updated"
)

type Entity interface {
	Key() *datastore.Key
	NewKey() *datastore.Key
	Kind() string
}

type DSKitEntityOptions struct {
	CreatedEnabled bool
	CreatedIndexed bool
	UpdatedEnabled bool
	UpdatedIndexed bool
}

func DefaultDSKitEntityOptions() *DSKitEntityOptions {
	return &DSKitEntityOptions{
		CreatedEnabled: true,
		CreatedIndexed: false,
		UpdatedEnabled: false,
		UpdatedIndexed: false,
	}
}

type DSKitEntity interface {
	Entity

	Options() *DSKitEntityOptions
}

type DSKitEntityImpl struct {
	Key     *datastore.Key `json:"-"`
	Created OptionalTime   `json:"-"`
	Updated OptionalTime   `json:"-"`

	options *DSKitEntityOptions
}

func (e *DSKitEntityImpl) LoadKey(key *datastore.Key) error {
	e.Key = key

	return nil
}

type propertyLoader func(p datastore.Property) error

func (e *DSKitEntityImpl) LoadProps(ps []datastore.Property, load propertyLoader) error {
	for i := range ps {
		switch ps[i].Name {
		case entityFieldCreated:
			e.Created = OptionalTimeFromProperty(ps[i])
		case entityFieldUpdated:
			e.Updated = OptionalTimeFromProperty(ps[i])
		default:
			if err := load(ps[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *DSKitEntityImpl) SaveProps(ps []datastore.Property) ([]datastore.Property, error) {
	opts := e.ensureOptions()

	var props []datastore.Property

	now := time.Now().UTC()

	if opts.CreatedEnabled && !e.Created.Valid {
		e.Created = OptionalTime{Time: now, Valid: true}
	}

	if e.Key != nil {
		props = append(
			props,
			datastore.Property{
				Name:    entityFieldKey,
				Value:   e.Key,
				NoIndex: false,
			},
		)
	}

	if opts.CreatedEnabled {
		props = append(props, e.Created.ToProperty(entityFieldCreated, opts.CreatedIndexed))
	}

	if opts.UpdatedEnabled && e.Updated.Valid {
		props = append(props, e.Updated.ToProperty(entityFieldUpdated, opts.UpdatedIndexed))
	}

	props = append(props, ps...)

	return props, nil
}

func (e *DSKitEntityImpl) PrepareForCreate() {
	opts := e.ensureOptions()
	now := time.Now().UTC()

	if opts.CreatedEnabled && !e.Created.Valid {
		e.Created = OptionalTime{Time: now, Valid: true}
	}

	if opts.UpdatedEnabled {
		e.Updated = OptionalTime{Time: now, Valid: true}
	}
}

func (e *DSKitEntityImpl) PrepareForUpdate() {
	opts := e.ensureOptions()

	if opts.UpdatedEnabled {
		e.Updated = OptionalTime{Time: time.Now().UTC(), Valid: true}
	}
}

func (e *DSKitEntityImpl) ensureOptions() *DSKitEntityOptions {
	if e.options == nil {
		e.options = DefaultDSKitEntityOptions()
	}

	return e.options
}

type entityCreatePreparer interface {
	PrepareForCreate()
}

type entityUpdatePreparer interface {
	PrepareForUpdate()
}

func touchEntityForCreate(entity any) {
	if preparer, ok := entity.(entityCreatePreparer); ok {
		if rv := reflect.ValueOf(preparer); rv.Kind() == reflect.Ptr && rv.IsNil() {
			return
		}

		preparer.PrepareForCreate()
	}
}

func touchEntityForUpdate(entity any) {
	if preparer, ok := entity.(entityUpdatePreparer); ok {
		if rv := reflect.ValueOf(preparer); rv.Kind() == reflect.Ptr && rv.IsNil() {
			return
		}

		preparer.PrepareForUpdate()
	}
}
