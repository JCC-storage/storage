package db2

import (
	"context"
	"fmt"
	"reflect"

	"gitlink.org.cn/cloudream/common/utils/serder"
	"gorm.io/gorm/schema"
)

type UnionSerializer struct {
}

func (UnionSerializer) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, dbValue interface{}) error {
	fieldValue := reflect.New(field.FieldType)
	if dbValue != nil {
		var data []byte
		switch v := dbValue.(type) {
		case []byte:
			data = v
		case string:
			data = []byte(v)
		default:
			return fmt.Errorf("failed to unmarshal JSONB value: %#v", dbValue)
		}

		err := serder.JSONToObjectExRaw(data, fieldValue.Interface())
		if err != nil {
			return err
		}
	}

	field.ReflectValueOf(ctx, dst).Set(fieldValue.Elem())
	return nil
}

func (UnionSerializer) Value(ctx context.Context, field *schema.Field, dst reflect.Value, fieldValue interface{}) (interface{}, error) {
	return serder.ObjectToJSONEx(fieldValue)
}

func init() {
	schema.RegisterSerializer("union", UnionSerializer{})
}
