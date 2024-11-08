package libutp

import (
	"testing"
)

func TestConnIdGenerator(t *testing.T) {
	genarator := NewConnIdGenerator()
	cid := genarator.GenCid("test-peer-id1", false)

	newCid := &ConnId{
		connSeed: cid.connSeed,
		recvId:   cid.recvId,
		sendId:   cid.sendId,
		peer:     cid.peer,
	}

	genarator.Remove(newCid)
	if genarator.(*DefaultConnIdGenerator).Cids[*cid] {
		t.Fatal("expected result")
	}
}
