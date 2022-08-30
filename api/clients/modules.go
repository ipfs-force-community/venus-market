package clients

import (
	logging "github.com/ipfs/go-log/v2"

	"github.com/ipfs-force-community/venus-common-utils/builder"
	"github.com/ipfs-force-community/venus-common-utils/metrics"
	"github.com/ipfs-force-community/venus-gateway/marketevent"
	gwTypes "github.com/ipfs-force-community/venus-gateway/types"

	"github.com/filecoin-project/venus-market/v2/api/clients/signer"
	"github.com/filecoin-project/venus-market/v2/config"

	v1api "github.com/filecoin-project/venus/venus-shared/api/chain/v1"
	v1Gateway "github.com/filecoin-project/venus/venus-shared/api/gateway/v1"
)

var log = logging.Logger("clients")

//var (
//	ReplaceWalletMethod = builder.NextInvoke()
//)

//func ConvertWalletToISinge(fullNode v1api.FullNode, signer signer.ISigner) error {
//	fullNodeStruct := fullNode.(*v1api.FullNodeStruct)
//	fullNodeStruct.IWalletStruct.Internal.WalletHas = func(p0 context.Context, p1 address.Address) (bool, error) {
//		return signer.WalletHas(p0, p1)
//	}
//	fullNodeStruct.IWalletStruct.Internal.WalletSign = func(p0 context.Context, p1 address.Address, p2 []byte, p3 types2.MsgMeta) (*vCrypto.Signature, error) {
//		return signer.WalletSign(p0, p1, p2, p3)
//	}
//	return nil
//}

func NewMarketEvent(mctx metrics.MetricsCtx) (v1Gateway.IMarketEvent, error) {
	stream := marketevent.NewMarketEventStream(mctx, &localMinerValidator{}, gwTypes.DefaultConfig())
	return stream, nil
}

var ClientsOpts = func(server bool, mode string, msgCfg *config.Messager, signerCfg *config.Signer) builder.Option {
	opts := builder.Options(
		builder.Override(new(IMixMessage), NewMixMsgClient),
		builder.Override(new(signer.ISigner), signer.NewISignerClient(server)),
		builder.ApplyIf(
			func(s *builder.Settings) bool {
				return len(msgCfg.Url) > 0
			},
			builder.Override(new(IVenusMessager), MessagerClient)),
	)

	if server {
		return builder.Options(opts,
			builder.Override(new(v1api.FullNode), NodeClient),

			builder.ApplyIf(
				func(s *builder.Settings) bool {
					return mode == "solo"
				},
				builder.Override(new(v1Gateway.IMarketEvent), NewMarketEvent),
			),
		)
	}

	return builder.Options(opts,
		builder.Override(new(v1api.FullNode), NodeClient),
	)
}
