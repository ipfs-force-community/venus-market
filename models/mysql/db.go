package mysql

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"time"

	mtypes "github.com/filecoin-project/venus-messager/types"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/models/itf"
)

type MysqlRepo struct {
	*gorm.DB
}

func (r MysqlRepo) GetDb() *gorm.DB {
	return r.DB
}

func (r MysqlRepo) FundRepo() itf.FundRepo {
	return NewFundedAddressStateRepo(r.GetDb())
}

func (r MysqlRepo) MinerDealRepo() itf.MinerDealRepo {
	return NewMinerDealRepo(r.GetDb())
}

func (r MysqlRepo) PaychMsgInfoRepo() itf.PaychMsgInfoRepo {
	return NewMsgInfoRepo(r.GetDb())
}

func (r MysqlRepo) PaychChannelInfoRepo() itf.PaychChannelInfoRepo {
	return NewChannelInfoRepo(r.GetDb())
}

func (r MysqlRepo) StorageAskRepo() itf.IStorageAskRepo {
	return NewStorageAskRepo(r.GetDb())
}

func InitMysql(cfg *config.Mysql) (itf.Repo, error) {
	db, err := gorm.Open(mysql.Open(cfg.ConnectionString))

	if err != nil {
		return nil, xerrors.Errorf("[db connection failed] Database name: %s %w", cfg.ConnectionString, err)
	}

	db.Set("gorm:table_options", "CHARSET=utf8mb4")
	if cfg.Debug {
		db = db.Debug()
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(cfg.MaxOpenConn)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConn)
	d, err := time.ParseDuration(cfg.ConnMaxLifeTime)
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(d)

	r := &MysqlRepo{DB: db}

	return r, r.AutoMigrate(fundedAddressState{}, minerDeal{}, channelInfo{}, msgInfo{}, storageAsk{})
}

func parseCid(str string) (cid.Cid, error) {
	if len(str) == 0 {
		return cid.Undef, nil
	}

	return cid.Parse(str)
}

func decodeCid(c cid.Cid) string {
	if c == cid.Undef {
		return ""
	}
	return c.String()
}

func parseCidPtr(str string) (*cid.Cid, error) {
	if len(str) == 0 {
		return nil, nil
	}
	c, err := cid.Parse(str)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func decodeCidPtr(c *cid.Cid) string {
	if c == nil {
		return ""
	}
	return c.String()
}

var undefAddrStr = address.Undef.String()

func parseAddrPtr(str string) (*address.Address, error) {
	if str == undefAddrStr {
		return nil, nil
	}
	addr, err := address.NewFromString(str)
	if err != nil {
		return nil, err
	}
	return &addr, nil
}

func decodeAddrPtr(addr *address.Address) string {
	if addr == nil {
		return address.Undef.String()
	}
	return addr.String()
}

func convertBigInt(v big.Int) mtypes.Int {
	if v.Nil() {
		return mtypes.Zero()
	}
	return mtypes.NewFromGo(v.Int)
}
