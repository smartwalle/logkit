package redact

// Redactor 对字符串内容执行脱敏转换。实现必须可被并发调用。
type Redactor interface {
	Redact(string) string
}

// redactorsChain 是按顺序执行多个 Redactor 的组合。
type redactorsChain []Redactor

// Redact 按顺序执行链中的每个 Redactor。
func (c redactorsChain) Redact(text string) string {
	for _, redactor := range c {
		text = redactor.Redact(text)
	}
	return text
}

// Chain 将多个 Redactor 按传入顺序组合为一个 Redactor。
func Chain(redactors ...Redactor) Redactor {
	chain := make(redactorsChain, 0, len(redactors))
	for _, redactor := range redactors {
		if redactor != nil {
			chain = append(chain, redactor)
		}
	}

	switch len(chain) {
	case 0:
		return nil
	case 1:
		return chain[0]
	default:
		return chain
	}
}
