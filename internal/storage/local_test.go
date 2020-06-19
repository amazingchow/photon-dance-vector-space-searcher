package storage

import (
	"crypto/sha256"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/amazingchow/engine-vector-space-search-service/api"
	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
)

func TestLocalStorageReadOp(t *testing.T) {
	p := NewLocalStorage("fixtures/local")

	err := p.Init()
	assert.Empty(t, err)

	file := &common.File{
		Type: pb.DocType_TextDoc,
		Name: "三部门开展三大粮食作物完全成本保险和收入保险试点工作",
	}
	expectedBody := []string{
		"财金〔2018〕93号",
		"内蒙古、辽宁、安徽、山东、河南、湖北省（自治区）财政厅（局）、农业（农牧、农村经济）厅（局、委、办）、银保监局：",
		"按照2018年中央一号文件部署，为进一步提升农业保险保障水平，推动农业保险转型升级，探索完善市场化的农业生产风险分散机制，现就开展三大粮食作物完全成本保险和收入保险试点工作通知如下：",
		"一、从2018年开始，用3年时间，在6个省份，每个省份选择4个产粮大县，面向规模经营农户和小农户，开展创新和完善农业保险政策试点，推动农业保险保障水平覆盖全部农业生产成本，或开展收入保险。",
		"二、中央层面建立财政部、农业农村部、银保监会共同组成的试点指导小组。试点地区省级财政部门应会同相关部门，将试点方案报财政部、农业农村部、银保监会备案后组织实施。试点方案内容主要包括：试点县情况、保险方案、补贴方案、保障措施、农业生产成本数据以及其他有必要进行说明的材料。",
		"三、省级财政部门应于2018年8月30日前，将试点资金申请报告报财政部，财政部根据各地报送的试点申请情况，于2018年9月30日前下达2018年试点资金，并在2019年统一结算。以后年度试点资金申请程序及时间按照《中央财政农业保险保险费补贴管理办法》（财金〔2016〕123号）执行。同时，直接物化成本保险、大灾保险、完全成本保险（收入保险）保费补贴资金申请要按对应文件要求分别填报，避免重复申请。",
		"四、各地要高度重视完全成本保险和收入保险试点工作，尤其注重做好对农户的及时足额理赔和财政补贴资金安全性的监控。财政部将按照“双随机、一公开”等要求开展监督检查，对不能实现理赔到户的保险机构和地区，财政部将取消其试点资格；对以任何方式截留或骗取保费补贴资金的，财政部将责令其改正并追回相应保费补贴资金，视情况暂停其中央财政农业保险保险费补贴资格等，并按照《预算法》《公务员法》《监察法》《财政违法行为处罚处分条例》等有关规定，对相关责任人员依法追究责任，给予相应处罚。",
		"现将《中央财政三大粮食作物完全成本保险和收入保险试点工作方案》（附件1）印发给你们，请认真执行。执行中如有问题，请及时报告。财政部将会同农业农村部、银保监会等相关部门，及时总结试点经验，不断完善相关政策。",
		"附件：",
		"1.三大粮食作物完全成本保险和收入保险试点工作方案",
		"2.2018年三大粮食作物完全成本保险和收入保险试点保费补贴情况表",
		"财政部 农业农村部 银保监会",
		"2018年8月20日",
	}

	path, err := p.Readable(file)
	assert.Empty(t, err)
	assert.Equal(t, "fixtures/local/text/三部门开展三大粮食作物完全成本保险和收入保险试点工作.txt", path)

	path, err = p.Get(file)
	assert.Empty(t, err)
	assert.Equal(t, "fixtures/local/text/三部门开展三大粮食作物完全成本保险和收入保险试点工作.txt", path)

	assert.Equal(t, len(expectedBody), len(file.Body))
	for line := range expectedBody {
		assert.Equal(t, expectedBody[line], file.Body[line])
	}

	err = p.Destroy()
	assert.Empty(t, err)
}

func TestLocalStorageWriteOp(t *testing.T) {
	p := NewLocalStorage("fixtures/local")

	err := p.Init()
	assert.Empty(t, err)

	fileBk := &common.File{
		Type: pb.DocType_TextDoc,
		Name: "三部门开展三大粮食作物完全成本保险和收入保险试点工作-备份",
		Body: []string{
			"财金〔2018〕93号",
			"内蒙古、辽宁、安徽、山东、河南、湖北省（自治区）财政厅（局）、农业（农牧、农村经济）厅（局、委、办）、银保监局：",
			"按照2018年中央一号文件部署，为进一步提升农业保险保障水平，推动农业保险转型升级，探索完善市场化的农业生产风险分散机制，现就开展三大粮食作物完全成本保险和收入保险试点工作通知如下：",
			"一、从2018年开始，用3年时间，在6个省份，每个省份选择4个产粮大县，面向规模经营农户和小农户，开展创新和完善农业保险政策试点，推动农业保险保障水平覆盖全部农业生产成本，或开展收入保险。",
			"二、中央层面建立财政部、农业农村部、银保监会共同组成的试点指导小组。试点地区省级财政部门应会同相关部门，将试点方案报财政部、农业农村部、银保监会备案后组织实施。试点方案内容主要包括：试点县情况、保险方案、补贴方案、保障措施、农业生产成本数据以及其他有必要进行说明的材料。",
			"三、省级财政部门应于2018年8月30日前，将试点资金申请报告报财政部，财政部根据各地报送的试点申请情况，于2018年9月30日前下达2018年试点资金，并在2019年统一结算。以后年度试点资金申请程序及时间按照《中央财政农业保险保险费补贴管理办法》（财金〔2016〕123号）执行。同时，直接物化成本保险、大灾保险、完全成本保险（收入保险）保费补贴资金申请要按对应文件要求分别填报，避免重复申请。",
			"四、各地要高度重视完全成本保险和收入保险试点工作，尤其注重做好对农户的及时足额理赔和财政补贴资金安全性的监控。财政部将按照“双随机、一公开”等要求开展监督检查，对不能实现理赔到户的保险机构和地区，财政部将取消其试点资格；对以任何方式截留或骗取保费补贴资金的，财政部将责令其改正并追回相应保费补贴资金，视情况暂停其中央财政农业保险保险费补贴资格等，并按照《预算法》《公务员法》《监察法》《财政违法行为处罚处分条例》等有关规定，对相关责任人员依法追究责任，给予相应处罚。",
			"现将《中央财政三大粮食作物完全成本保险和收入保险试点工作方案》（附件1）印发给你们，请认真执行。执行中如有问题，请及时报告。财政部将会同农业农村部、银保监会等相关部门，及时总结试点经验，不断完善相关政策。",
			"附件：",
			"1.三大粮食作物完全成本保险和收入保险试点工作方案",
			"2.2018年三大粮食作物完全成本保险和收入保险试点保费补贴情况表",
			"财政部 农业农村部 银保监会",
			"2018年8月20日",
		},
	}

	path, err := p.Writable(fileBk)
	assert.Empty(t, err)
	assert.Equal(t, "fixtures/local/text/三部门开展三大粮食作物完全成本保险和收入保险试点工作-备份.txt", path)

	path, err = p.Put(fileBk)
	assert.Empty(t, err)
	assert.Equal(t, "fixtures/local/text/三部门开展三大粮食作物完全成本保险和收入保险试点工作-备份.txt", path)

	// checksum
	file1 := &common.File{
		Type: pb.DocType_TextDoc,
		Name: "三部门开展三大粮食作物完全成本保险和收入保险试点工作",
	}

	f1, err := os.Open(p.LocalPath(file1))
	if err != nil {
		assert.Empty(t, err)
	}
	defer f1.Close()

	h1 := sha256.New()
	_, err = io.Copy(h1, f1)
	assert.Empty(t, err)

	file2 := &common.File{
		Type: pb.DocType_TextDoc,
		Name: "三部门开展三大粮食作物完全成本保险和收入保险试点工作-备份",
	}

	f2, err := os.Open(p.LocalPath(file2))
	if err != nil {
		assert.Empty(t, err)
	}
	defer f2.Close()

	h2 := sha256.New()
	_, err = io.Copy(h2, f2)
	assert.Empty(t, err)

	assert.Equal(t, h1.Sum(nil), h2.Sum(nil))

	err = p.Delete(fileBk)
	assert.Empty(t, err)

	err = p.Destroy()
	assert.Empty(t, err)
}
