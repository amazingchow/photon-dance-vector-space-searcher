package common

// WebStationType 网站类型
type WebStationType int

const (
	// MOFRPC 中华人民共和国财政部
	MOFRPC WebStationType = 1
)

// Packet is the wrapper of one input text file.
type Packet struct {
	Local      bool
	WebStation WebStationType
	Title      string
	Location   string
}

// Pipeline is used to transfer input text files.
type Pipeline (chan *Packet)
