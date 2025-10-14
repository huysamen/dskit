package dskit

import (
	"time"

	"cloud.google.com/go/datastore"
)

type OptionalTime struct {
	Time  time.Time `json:"time"`
	Valid bool      `json:"valid"`
}

func (ot OptionalTime) IsZero() bool {
	return !ot.Valid || ot.Time.IsZero()
}

func (ot OptionalTime) Ptr() *time.Time {
	if !ot.Valid {
		return nil
	}

	return &ot.Time
}

func (ot OptionalTime) ToProperty(name string, indexed bool) datastore.Property {
	var value any

	if ot.Valid {
		value = ot.Time
	}

	return datastore.Property{
		Name:    name,
		Value:   value,
		NoIndex: !indexed,
	}
}

func OptionalTimeFromProperty(prop datastore.Property) OptionalTime {
	if prop.Value == nil {
		return OptionalTime{}
	}

	if t, ok := prop.Value.(time.Time); ok {
		return OptionalTime{
			Time:  t,
			Valid: true,
		}
	}

	return OptionalTime{}
}
