package query

import (
	"fmt"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

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
