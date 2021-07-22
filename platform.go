package szBinary

const (
	PlatformID1 uint16 = 1 //现货集中竞价交易平台
	PlatformID2 uint16 = 2 //综合金融服务平台
	PlatformID3 uint16 = 3 //非交易处理平台
	PlatformID4 uint16 = 4 //衍生品集中竞价交易平台
	PlatformID5 uint16 = 5 //国际市场互联平台
	PlatformID6 uint16 = 6 //固定收益交易平台
)

const (
	PlatformState_PreOpen uint16 = 0  //未开放
	PlatformState_OpenUpComing uint16 = 1  //即将开放
	PlatformState_Open uint16 = 2  //开放
	PlatformState_Halt uint16 = 3  //暂停开放
	PlatformState_Close uint16 = 4  //关闭
)
