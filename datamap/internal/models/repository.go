package models

import (
	"gorm.io/gorm"
)

// Repository 通用接口
type Repository interface {
	Create(value interface{}) error
	Update(value interface{}) error
	Delete(value interface{}, id uint) error
	GetByID(id uint, out interface{}) error
	GetAll(out interface{}) error
}

// GormRepository 基于 GORM 的通用实现
type GormRepository struct {
	db *gorm.DB
}

func NewGormRepository(db *gorm.DB) *GormRepository {
	return &GormRepository{db: db}
}

func (r *GormRepository) Create(value interface{}) error {
	return r.db.Create(value).Error
}

func (r *GormRepository) Update(value interface{}) error {
	return r.db.Save(value).Error
}

func (r *GormRepository) Delete(value interface{}, id uint) error {
	return r.db.Delete(value, id).Error
}

func (r *GormRepository) GetByID(id uint, out interface{}) error {
	return r.db.First(out, id).Error
}

func (r *GormRepository) GetAll(out interface{}) error {
	return r.db.Find(out).Error
}
