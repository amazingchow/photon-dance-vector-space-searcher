package mysql

import (
	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
)

// Client MySQL客户端
type Client struct {
	cfg *conf.MySQLConfig
	db  *gorm.DB
}

// NewClient 新建MySQL客户端.
func NewClient(cfg *conf.MySQLConfig) *Client {
	return &Client{
		cfg: cfg,
	}
}

// Doc 数据库schema定义
type Doc struct {
	ID    int64
	DocID string `gorm:"size:8;column:doc_id;not null"`
	Title string `gorm:"size:255;column:title;not null"`
}

// Setup 初始化MySQL连接服务.
func (cli *Client) Setup() {
	var err error

	args := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		cli.cfg.User, cli.cfg.Password, cli.cfg.Host, cli.cfg.Port, cli.cfg.DB)

	cli.db, err = gorm.Open("mysql", args)
	if err != nil {
		log.Fatal().Err(err)
	}
}

// Close 关闭MySQL连接服务.
func (cli *Client) Close() {
	cli.db.Close()
}

// Query 根据文档ID查找数据库中对应的文档名.
func (cli *Client) Query(docID string) string {
	var doc Doc

	cli.db.Where("doc_id = ?", docID).First(&doc)

	return doc.Title
}
