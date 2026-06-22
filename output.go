package wpn

type Output int

const (
	OutputFail = iota
	OutputSuccessful
)

func (o Output) String() string {
	switch o {
	case 0:
		return "Failed"
	case 1:
		return "Completed"
	default:
		return "Unknown result"
	}
}
