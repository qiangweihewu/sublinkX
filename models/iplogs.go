package models

type SubLogs struct {
	ID            int
	IP            string
	Date          string
	Addr          string
	Count         int
	SubcriptionID int
}

// Add 添加IP
func (iplog *SubLogs) Add() error {
	return DB.Create(iplog).Error
}

// 查找IP
func (iplog *SubLogs) Find(id int) error {
	return DB.Where("ip = ? and subcription_id  = ?", iplog.IP, id).First(iplog).Error
}

// Update 更新IP
func (iplog *SubLogs) Update() error {
	// 只按 id 更新：按 ip 匹配会把同一 IP 在其他订阅下的记录一起改掉
	return DB.Where("id = ?", iplog.ID).Updates(iplog).Error
}

// List 获取IP列表
func (iplog *SubLogs) List() ([]SubLogs, error) {
	var iplogs []SubLogs
	err := DB.Find(&iplogs).Error
	if err != nil {
		return nil, err
	}
	return iplogs, nil
}

// Del 删除IP
func (iplog *SubLogs) Del() error {
	return DB.Delete(iplog).Error
}
