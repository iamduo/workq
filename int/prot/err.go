package prot

var (
	ErrNotFound   = NewError("NOT-FOUND", "")
	ErrTimeout    = NewError("TIMEOUT", "")
	ErrUnknownCmd = NewClientErr("Unknown command")
)

type errorString struct {
	code string
	text string
}

func (e errorString) Error() string {
	if e.text != "" {
		return e.code + " " + e.text + "\r\n"
	}

	return e.code + "\r\n"
}

type ErrClient struct {
	errorString
}

type ErrServer struct {
	errorString
}

func NewClientErr(text string) error {
	err := &ErrClient{}
	err.code = "CLIENT-ERROR"
	err.text = text
	return err
}

func NewServerErr(text string) error {
	err := &ErrServer{}
	err.code = "SERVER-ERROR"
	err.text = text
	return err
}

func NewError(code string, text string) error {
	return &errorString{
		code: code,
		text: text,
	}
}
