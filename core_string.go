package nutshttp

import (
	"errors"
	nutsdb "github.com/finddiff/nutsDBMD"
)

// Update handle insert and update operation
func (c *core) Update(bucket string, key string, value string, ttl uint32) error {
	err := c.db.Update(func(tx *nutsdb.Tx) error {
		err := tx.Put(bucket, []byte(key), []byte(value), ttl)
		return err
	})
	return err
}

type BatchItem struct {
	Key   string `json:"key" binding:"required"`
	Value string `json:"value" binding:"required"`
	Ttl   uint32 `json:"ttl"`
}

// BatchUpdate handle insert and update operation
func (c *core) BatchUpdate(bucket string, items []BatchItem) error {
	err := c.db.Update(func(tx *nutsdb.Tx) error {
		errString := ""
		for _, item := range items {
			//fmt.Printf("bucket:%v, item.Key:%v, item.Value:%v, item.Ttl:%v\n", bucket, item.Key , item.Value, item.Ttl)
			err := tx.Put(bucket, []byte(item.Key), []byte(item.Value), item.Ttl)
			if err != nil {
				errString += err.Error()
			}
		}
		if errString != "" {
			return errors.New(errString)
		}
		return nil
	})
	return err
}

// Swaps handle insert and update operation
func (c *core) Swaps(bucket string, key string, oldValue string, value string, ttl uint32) error {
	err := c.db.Update(func(tx *nutsdb.Tx) error {
		entry, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		orgValue := string(entry.Value)
		//fmt.Printf("orgValue:%v, oldValue:%v, value:%v, orgValue == oldValue:%v\n", orgValue, oldValue , value, orgValue == oldValue)
		if orgValue == oldValue {
			err = tx.Put(bucket, []byte(key), []byte(value), ttl)
		} else {
			return nutsdb.ErrNotFoundKey
		}
		return err
	})
	return err
}

// Delete handle delete operation
func (c *core) Delete(bucket string, key string) error {
	err := c.db.Update(func(tx *nutsdb.Tx) error {
		err := tx.Delete(bucket, []byte(key))
		return err
	})
	return err
}

// Get handle get key operation
func (c *core) Get(bucket string, key string) (value string, err error) {
	err = c.db.View(func(tx *nutsdb.Tx) error {
		entry, err := tx.Get(bucket, []byte(key))
		if err != nil {
			return err
		}
		value = string(entry.Value)
		return nil
	})
	return value, err
}

func (c *core) PrefixScan(bucket string, prefix string, offSet int, limNum int) (entries nutsdb.Entries, err error) {
	err = c.db.View(func(tx *nutsdb.Tx) error {
		entries, _, err = tx.PrefixScan(bucket, []byte(prefix), offSet, limNum)
		return err
	})
	return entries, err
}

func (c *core) PrefixSearchScan(bucket, prefix string, reg string, offSet int, limNum int) (entries nutsdb.Entries, err error) {
	err = c.db.View(func(tx *nutsdb.Tx) error {
		entries, _, err = tx.PrefixSearchScan(bucket, []byte(prefix), reg, offSet, limNum)
		return err
	})
	return entries, err
}

func (c *core) RangeScan(bucket string, start string, end string) (entries nutsdb.Entries, err error) {
	err = c.db.View(func(tx *nutsdb.Tx) error {
		entries, err = tx.RangeScan(bucket, []byte(start), []byte(end))
		return err
	})
	return entries, err
}

func (c *core) GetAll(bucket string) (entries nutsdb.Entries, err error) {
	err = c.db.View(func(tx *nutsdb.Tx) error {
		entries, err = tx.GetAll(bucket)
		return err
	})
	return entries, err
}

func (c *core) DeleteOldFiles(count int) (err error) {
	err = c.db.DeleteOldFiles(count)
	return err
}
