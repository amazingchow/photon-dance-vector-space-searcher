package common

import (
	pb "github.com/amazingchow/engine-vector-space-search-service/api"
)

// LanguageType 语种类型
type LanguageType int

const (
	// LanguageTypeEnglish 英语语种类型
	LanguageTypeEnglish LanguageType = 0
	// LanguageTypeChinsese 中文语种类型
	LanguageTypeChinsese LanguageType = 1
)

var (
	// FileType2FileTypeName 文件类型到文件类型名之间的映射.
	FileType2FileTypeName = map[pb.FileType]string{
		pb.FileType_HTMLFile: "html",
		pb.FileType_TextFile: "text",
	}

	// FileType2FileSuffix 文件类型到文件后缀之间的映射.
	FileType2FileSuffix = map[pb.FileType]string{
		pb.FileType_HTMLFile: "html",
		pb.FileType_TextFile: "txt",
	}
)

// File 通用文件定义.
type File struct {
	Type pb.FileType
	Name string
	Body []string
}

// WordsWrapper 封装words.
type WordsWrapper struct {
	Words []string
}

// ConcordanceWrapper 封装concordance.
type ConcordanceWrapper struct {
	Concordance map[string]uint32
}

// PacketChannel is used to transfer pb.Packet.
type PacketChannel (chan *pb.Packet)

// ConcordanceChannel is used to transfer ConcordanceWrapper.
type ConcordanceChannel (chan *ConcordanceWrapper)
