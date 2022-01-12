package retrievalprovider

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/paychmgr"
	"github.com/filecoin-project/venus-market/types"
	v1api "github.com/filecoin-project/venus/venus-shared/api/chain/v1"
	"math"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/dtutils"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/migrations"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/venus-market/models/repo"
	"github.com/filecoin-project/venus-market/network"
	logging "github.com/ipfs/go-log/v2"
)

var queryTimeout = 5 * time.Second

var log = logging.Logger("retrievaladapter")

type IRetrievalProvider interface {
	Stop() error
	Start(context.Context) error
	ListDeals(context.Context) (map[retrievalmarket.ProviderDealIdentifier]*types.ProviderDealState, error)
}

// RetrievalProvider is the production implementation of the RetrievalProvider interface
type RetrievalProvider struct {
	dataTransfer     network.ProviderDataTransfer
	network          rmnet.RetrievalMarketNetwork
	requestValidator *ProviderRequestValidator
	reValidator      *ProviderRevalidator
	disableNewDeals  bool
	dagStore         stores.DAGStoreWrapper
	stores           *stores.ReadOnlyBlockstores

	retrievalDealRepo repo.IRetrievalDealRepo
	storageDealRepo   repo.StorageDealRepo

	retrievalStreamHandler *RetrievalStreamHandler
}

// NewProvider returns a new retrieval Provider
func NewProvider(network rmnet.RetrievalMarketNetwork,
	dagStore stores.DAGStoreWrapper,
	dataTransfer network.ProviderDataTransfer,
	fullNode v1api.FullNode,
	payAPI *paychmgr.PaychAPI,
	repo repo.Repo,
	cfg *config.MarketConfig,
) (*RetrievalProvider, error) {
	storageDealsRepo := repo.StorageDealRepo()
	retrievalDealRepo := repo.RetrievalDealRepo()
	cidInfoRepo := repo.CidInfoRepo()
	retrievalAskRepo := repo.RetrievalAskRepo()

	pieceInfo := &PieceInfo{cidInfoRepo: cidInfoRepo, dealRepo: storageDealsRepo}
	p := &RetrievalProvider{
		dataTransfer:           dataTransfer,
		network:                network,
		dagStore:               dagStore,
		retrievalDealRepo:      retrievalDealRepo,
		storageDealRepo:        storageDealsRepo,
		stores:                 stores.NewReadOnlyBlockstores(),
		retrievalStreamHandler: NewRetrievalStreamHandler(retrievalAskRepo, retrievalDealRepo, storageDealsRepo, pieceInfo, address.Address(cfg.RetrievalPaymentAddress.Addr)),
	}

	retrievalHandler := NewRetrievalDealHandler(&providerDealEnvironment{p}, retrievalDealRepo, storageDealsRepo)
	p.requestValidator = NewProviderRequestValidator(address.Address(cfg.RetrievalPaymentAddress.Addr), storageDealsRepo, retrievalDealRepo, retrievalAskRepo, pieceInfo)
	transportConfigurer := dtutils.TransportConfigurer(network.ID(), &providerStoreGetter{retrievalDealRepo, p.stores})
	p.reValidator = NewProviderRevalidator(fullNode, payAPI, retrievalDealRepo, retrievalHandler)

	var err error
	if p.disableNewDeals {
		err = p.dataTransfer.RegisterVoucherType(&migrations.DealProposal0{}, p.requestValidator)
		if err != nil {
			return nil, err
		}
		err = p.dataTransfer.RegisterRevalidator(&migrations.DealPayment0{}, p.reValidator)
		if err != nil {
			return nil, err
		}
	} else {
		err = p.dataTransfer.RegisterVoucherType(&retrievalmarket.DealProposal{}, p.requestValidator)
		if err != nil {
			return nil, err
		}
		err = p.dataTransfer.RegisterVoucherType(&migrations.DealProposal0{}, p.requestValidator)
		if err != nil {
			return nil, err
		}

		err = p.dataTransfer.RegisterRevalidator(&retrievalmarket.DealPayment{}, p.reValidator)
		if err != nil {
			return nil, err
		}
		err = p.dataTransfer.RegisterRevalidator(&migrations.DealPayment0{}, NewLegacyRevalidator(p.reValidator))
		if err != nil {
			return nil, err
		}

		err = p.dataTransfer.RegisterVoucherResultType(&retrievalmarket.DealResponse{})
		if err != nil {
			return nil, err
		}

		err = p.dataTransfer.RegisterTransportConfigurer(&retrievalmarket.DealProposal{}, transportConfigurer)
		if err != nil {
			return nil, err
		}
	}
	err = p.dataTransfer.RegisterVoucherResultType(&migrations.DealResponse0{})
	if err != nil {
		return nil, err
	}
	err = p.dataTransfer.RegisterTransportConfigurer(&migrations.DealProposal0{}, transportConfigurer)
	if err != nil {
		return nil, err
	}
	datatransferProcess := NewDataTransferHandler(retrievalHandler, retrievalDealRepo)
	dataTransfer.SubscribeToEvents(ProviderDataTransferSubscriber(datatransferProcess))
	return p, nil
}

// Stop stops handling incoming requests.
func (p *RetrievalProvider) Stop() error {
	return p.network.StopHandlingRequests()
}

// Start begins listening for deals on the given host.
// Start must be called in order to accept incoming deals.
func (p *RetrievalProvider) Start(ctx context.Context) error {
	return p.network.SetDelegate(p.retrievalStreamHandler)
}

// ListDeals lists all known retrieval deals
func (p *RetrievalProvider) ListDeals(ctx context.Context) (map[retrievalmarket.ProviderDealIdentifier]*types.ProviderDealState, error) {
	deals, err := p.retrievalDealRepo.ListDeals(ctx, 0, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	dealMap := make(map[retrievalmarket.ProviderDealIdentifier]*types.ProviderDealState)
	for _, deal := range deals {
		dealMap[retrievalmarket.ProviderDealIdentifier{Receiver: deal.Receiver, DealID: deal.ID}] = deal
	}
	return dealMap, nil
}

var _ IRetrievalProvider = (*RetrievalProvider)(nil)
