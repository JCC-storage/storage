package tempstore

type TempStore interface {
	CreateTemp()
	Commited(objectName string)
	Drop(objectName string)
}
