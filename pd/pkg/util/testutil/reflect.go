package testutil

import (
	"reflect"
)

func GetAllFields(i any) (fields []string) {
	t := reflect.TypeOf(i)
	for i := 0; i < t.NumField(); i++ {
		fields = append(fields, t.Field(i).Name)
	}
	return
}
