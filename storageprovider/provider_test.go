package storageprovider

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/venus/venus-shared/types"
	"github.com/golang/mock/gomock"
	"github.com/ipfs-force-community/droplet/v2/api/clients"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

func TestWaitMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := clients.NewMockIMixMessage(ctrl)

	pna := &ProviderNodeAdapter{
		msgClient:   m,
		pendingMsgs: map[cid.Cid]*pendingMsg{},
	}

	m.EXPECT().WaitMsg(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, msg cid.Cid, confidence uint64, limit abi.ChainEpoch, allowReplaced bool) (*types.MsgLookup, error) {
			time.Sleep(time.Millisecond * 10)
			return nil, fmt.Errorf("error")
		})

	for i := 0; i < 1000; i++ {
		res, err := pna.WaitForPublishDeals(context.Background(), cid.Cid{}, types.DealProposal{})
		assert.Error(t, err)
		assert.Nil(t, res)
		fmt.Println(i)
	}

	var wg sync.WaitGroup
	ch := make(chan struct{}, 100)
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		ch <- struct{}{}
		go func() {
			res, err := pna.WaitForPublishDeals(context.Background(), cid.Cid{}, types.DealProposal{})
			assert.Error(t, err)
			assert.Nil(t, res)
			fmt.Println(i)
			<-ch
			wg.Done()
		}()
	}
	wg.Wait()
}
