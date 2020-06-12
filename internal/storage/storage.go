package storage

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

// Persister 持久化接口定义.
type Persister interface {
	Init() (err error)
	Destroy() (err error)
	Writable() (path string, err error)
	Put() (string, error)
	Readable() (path string, err error)
	Get() (string, error)
	Abort() error
	Delete() error
}
