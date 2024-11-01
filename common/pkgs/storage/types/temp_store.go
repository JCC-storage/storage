package types

type TempStore interface {
	StorageComponent
	CreateTemp() string
	Commited(objectName string)
	Drop(objectName string)
}
