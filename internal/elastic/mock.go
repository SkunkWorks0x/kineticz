package elastic

import "context"

type Mock struct {
	LookupContractFn func(ctx context.Context, q ContractQuery) (*ContractContext, error)
}

func (m *Mock) LookupContract(ctx context.Context, q ContractQuery) (*ContractContext, error) {
	if m.LookupContractFn != nil {
		return m.LookupContractFn(ctx, q)
	}
	return &ContractContext{}, nil
}
