package common

// FileType 文件类型.
type FileType int

const (
	// HTMLFile HTML文件.
	HTMLFile FileType = 1
	// TextFile 文本文件.
	TextFile FileType = 2
)

var (
	// FileType2FileTypeName 文件类型到文件类型名之间的映射.
	FileType2FileTypeName = map[FileType]string{
		HTMLFile: "html",
		TextFile: "text",
	}

	// FileType2FileSuffix 文件类型到文件后缀之间的映射.
	FileType2FileSuffix = map[FileType]string{
		HTMLFile: "html",
		TextFile: "txt",
	}
)

// File 通用文件定义.
type File struct {
	Type FileType
	Name string
	Body []string
}

// WebStationType 网站类型
type WebStationType int

const (
	// MOFRPC 中华人民共和国财政部
	MOFRPC WebStationType = 1
)

// Packet is the wrapper of one input text file.
type Packet struct {
	WebStation WebStationType
	FileType   FileType
	FileTitle  string
}

// Pipeline is used to transfer input text files.
type Pipeline (chan *Packet)
