package common

import (
	"fmt"
	"runtime"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/mihomo/log"
)

type Uid struct {
	Base
	uids    utils.IntRanges[uint32]
	oUid    string
	adapter string
}

func NewUid(oUid, adapter string) (*Uid, error) {
	if !(runtime.GOOS == "linux" || runtime.GOOS == "android") {
		return nil, fmt.Errorf("uid rule not support this platform")
	}

	uidRange, err := utils.NewUnsignedRanges[uint32](oUid)
	if err != nil {
		return nil, fmt.Errorf("%w, %w", errPayload, err)
	}

	if len(uidRange) == 0 {
		return nil, errPayload
	}
	if features.CMFA {
		log.Warnln("[UID] rule UID,%s never matches on Android: use PROCESS-NAME with the app package name", oUid)
	}
	return &Uid{
		Base:    Base{},
		adapter: adapter,
		oUid:    oUid,
		uids:    uidRange,
	}, nil
}

func (u *Uid) RuleType() C.RuleType {
	return C.Uid
}

func (u *Uid) Match(metadata *C.Metadata, helper C.RuleMatchHelper) (bool, string) {
	// clod: the Android client fills the package name, not the uid, so the
	// rule cannot match there; it stays silent instead of looking the process
	// up and logging a warning for every connection.
	if features.CMFA {
		return false, ""
	}
	if helper.FindProcess != nil {
		helper.FindProcess()
	}
	if metadata.Uid != 0 {
		if u.uids.Check(metadata.Uid) {
			return true, u.adapter
		}
	}
	log.Warnln("[UID] could not get uid from %s", metadata.String())
	return false, ""
}

func (u *Uid) Adapter() string {
	return u.adapter
}

func (u *Uid) Payload() string {
	return u.oUid
}

var _ C.Rule = (*Uid)(nil)
