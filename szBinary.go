package szBinary

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/logging"
	"github.com/panjf2000/gnet/pool/goroutine"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	seq "sync/atomic"
	"time"
)

const msgtypeLogon uint32 = 1
const msgtypeLogout uint32 = 2
const msgtypeHeartbeat uint32 = 3
const msgtypeBusinessReject uint32 = 4
const msgtypeReportSynchronization uint32 = 5
const msgtypePlatformState uint32 = 6
const msgtypeReportFinished uint32 = 7
const msgtypePlatformInfo uint32 = 9

type msgDef struct {
	Offset int64
	Size int
}

type SzBinaryServer struct {
	*gnet.EventServer
	connected *atomic.Bool
	logon *atomic.Bool
	senderCompID string
	targetCompID string
	heartBeatInt uint32
	pool *goroutine.Pool
	receiveTime *atomic.Time
	sendTime *atomic.Time
	client gnet.Conn
	onBusinessRequestCallback func(msgtype uint32, body []byte)
	running *atomic.Bool
	runningChan chan bool
	port int
	platformID uint16
	partitionNos []uint32
	// log文件
	logfile logging.Logger
	// log flush函数
	logFlush func() error
	// sequence内存,key的格式为"平台编号_分区编号", value为该分区的最大回报编号
	sequence map[string]*uint64
	sequenceFile *os.File
	sequenceFileName string
	// data文件
	dataFile *os.File
	dataFileName string
	// data索引, key的格式为"平台编号_分区编号_回报序号", value为data文件的偏移量及数据长度
	indexCache map[string]msgDef
	indexFile *os.File
	indexFileName string
}

func (es *SzBinaryServer) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	var err error
	es.logfile, es.logFlush, err = logging.CreateLoggerAsLocalFile(fmt.Sprintf("./logs/%s-%s.log", es.senderCompID, es.targetCompID), zapcore.DebugLevel)
	if err != nil {
		es.logfile.Errorf("server startup failed: %s", err.Error())
		return gnet.Shutdown
	}
	es.logfile.Infof("server startup successfully listen on %s", srv.Addr.String())
	es.runningChan <- true
	return
}

func (es *SzBinaryServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	es.logfile.Infof("%s disconnected!", c.RemoteAddr().String())
	if c == es.client {
		es.client = nil
		es.logon.Store(false)
		es.connected.Store(false)
	}
	return
}

func (es *SzBinaryServer) OnShutdown(svr gnet.Server) {
	es.logfile.Infof("%s is going to shutdown", svr.Addr.String())
	es.logFlush()
	closeFile(es.indexFile)
	closeFile(es.dataFile)
	closeFile(es.sequenceFile)
}

func (es *SzBinaryServer) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	es.logfile.Infof("%s request to connect", c.RemoteAddr().String())
	if es.connected.Load() {
		// already connected, only one client allowed to be connected
		es.asyncSendToTarget(c, msgtypeLogout, logOut("another client already connected"))
		es.logfile.Errorf("another client already connected to this gateway: %s", es.client.RemoteAddr())
		c.Close()
		return
	}
	// connect
	es.connected.Store(true)
	es.client = c
	es.sendTime = atomic.NewTime(time.Now())
	es.receiveTime = atomic.NewTime(time.Now())
	es.logfile.Infof("%s connected!", c.RemoteAddr().String())
	return
}

func (es *SzBinaryServer) SendPlatformState(status uint16) (err error) {
	// 本消息用于新一代交易系统向 OMS 提供各平台的当前状态，便于 OMS 控制是否发送委托
	//消息体
	b := make([]byte, 2 + 2)
	binary.BigEndian.PutUint16(b, es.platformID)
	binary.BigEndian.PutUint16(b[2:4], status)
	return es.AsyncSend(msgtypePlatformState, b)
}

func (es *SzBinaryServer) SendReportFinish(partitionNo uint32, reportIndex uint64) (err error) {
	//TGW 使用该消息通知 OMS 本交易日当前平台对应分区回报发送完毕
	for k, v := range es.sequence {
		l := strings.Split(k,"_")
		platformIDStr := l[0]
		partitionNoStr := l[1]
		platformID, err := strconv.ParseUint(platformIDStr, 10, 16)
		if err != nil {
			return err
		}
		if platformID != uint64(es.platformID) {
			continue
		}
		partitionNo, err := strconv.ParseUint(partitionNoStr, 10, 32)
		if err != nil {
			return err
		}
		reportIndex := *v
		// 回报消息发送
		//消息体
		b := make([]byte, 4 + 8 + 2)
		binary.BigEndian.PutUint32(b, uint32(partitionNo))
		binary.BigEndian.PutUint64(b[4:12], reportIndex)
		binary.BigEndian.PutUint16(b[12:14], es.platformID)
		if err := es.AsyncSend(msgtypeReportFinished, b); err != nil {
			return err
		}
	}
	return nil
}

func (es *SzBinaryServer) SendPlatformInfo() (err error) {
	// 本消息用于 TGW 向 OMS 提供各平台的基本信息
	//消息体
	b := make([]byte, 2 + 4 + 4 * len(es.partitionNos))
	binary.BigEndian.PutUint16(b, es.platformID)
	binary.BigEndian.PutUint32(b[2:6], uint32(len(es.partitionNos)))
	for i:=0; i<len(es.partitionNos); i++ {
		binary.BigEndian.PutUint32(b[6+i*4:6+i*4+4], es.partitionNos[i])
	}
	return es.AsyncSend(msgtypePlatformInfo, b)
}

func (es *SzBinaryServer) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	es.pool.Submit(func() {
		es.logfile.Infof("incoming: from=%s, data=%v", c.RemoteAddr(), frame)
		es.receiveTime.Store(time.Now())
		msgType := binary.BigEndian.Uint32(frame[:4])
		bodyLen := binary.BigEndian.Uint32(frame[4:8])
		bodyBuff := frame[8:8+bodyLen]
		switch msgType {
		case msgtypeLogon:
			if !es.logon.Load() {
				senderCompID := Bytes2str(bodyBuff[:20])
				targetCompID := Bytes2str(bodyBuff[20:40])
				if es.senderCompID != strings.TrimSpace(targetCompID) || es.targetCompID != strings.TrimSpace(senderCompID) {
					es.AsyncSend(msgtypeLogout, logOut("senderCompID and targetCompID not match"))
					c.Close()
					return
				}
				HeartBtInt := binary.BigEndian.Uint32(bodyBuff[40:44])
				Password := Bytes2str(bodyBuff[44:60])
				DefaultApplVerID := Bytes2str(bodyBuff[60:92])
				es.AsyncSend(msgtypeLogon, logOn(es.senderCompID, es.targetCompID, HeartBtInt, Password, DefaultApplVerID))
				es.heartBeatInt = HeartBtInt
				es.logon.Store(true)
				// send platform status after logon successfully
				es.SendPlatformState(PlatformState_Open)
				// send platform info
				es.SendPlatformInfo()
				return
			}
			// duplicate logon
			es.asyncSendToTarget(c, msgtypeLogout, logOut("duplicate logon"))
			c.Close()
			return
		case msgtypeLogout:
			if !es.checkLogon(c) {
				c.Close()
				return
			}
			es.AsyncSend(msgtypeLogout, logOut(""))
			c.Close()
			return
		case msgtypeHeartbeat:
			if !es.checkLogon(c) {
				c.Close()
				return
			}
			return
		case msgtypeReportSynchronization:
			if !es.checkLogon(c) {
				c.Close()
				return
			}
			NoPartitions := binary.BigEndian.Uint32(bodyBuff[:4])
			for i:=uint32(0);i<NoPartitions;i++{
				PartitionNo := binary.BigEndian.Uint32(bodyBuff[4 + 12*i:8 + 12*i])
				ReportIndex := binary.BigEndian.Uint64(bodyBuff[8 + 12*i:16 + 12*i])
				if PartitionNo == 4 {
					ReportIndex = 11
				}
				es.logfile.Infof("回报同步请求，分区编号=%d, 起始序号=%d", PartitionNo, ReportIndex)
				for i:=0; i<1000; i++ {
					//每次同步一千条
					if msgdef, found := es.indexCache[fmt.Sprintf("%d_%d_%d", es.platformID, PartitionNo, ReportIndex+uint64(i))]; found {
						msg := make([]byte, msgdef.Size)
						if _, err := es.dataFile.ReadAt(msg, msgdef.Offset); err != nil {
							es.logfile.Errorf("unable to read from file: %s: %s", es.dataFileName, err.Error())
							break
						}
						es.sendBuff(msg)
					}
				}
			}
			return
		default:
			if !es.checkLogon(c) {
				c.Close()
				return
			}
			es.onBusinessRequestCallback(msgType, bodyBuff)
		}
	})
	return
}

func (es *SzBinaryServer) checkLogon(c gnet.Conn) bool {
	if !es.logon.Load() {
		es.asyncSendToTarget(c, msgtypeLogout, logOut("not logon yet, please logon first"))
		return false
	}
	return c == es.client
}

func (es *SzBinaryServer) Connected() bool {
	return es.connected.Load() && es.client != nil
}

func (es *SzBinaryServer) Tick() (delay time.Duration, action gnet.Action) {
	if es.Connected() {
		if time.Now().Sub(es.sendTime.Load()).Seconds() >= float64(es.heartBeatInt) {
			// need to send a heartbeat message to client
			es.AsyncSend(msgtypeHeartbeat, []byte{})
		}
	}
	if es.Connected() {
		if time.Now().Sub(es.receiveTime.Load()).Seconds() > float64(2 * es.heartBeatInt) {
			// over heartbeat time, close this tcp client
			es.client.Close()
		}
	}
	delay = time.Duration(es.heartBeatInt) * time.Second
	return
}

func (es *SzBinaryServer) sendBuff(buff []byte) (err error) {
	es.logfile.Infof("outgoing: target=%s, data=%v", es.client.RemoteAddr(), buff)
	if err = es.client.AsyncWrite(buff); err != nil {
		es.logfile.Errorf("response send failed: %s", err.Error())
		return err
	}
	return nil
}

func (es *SzBinaryServer) sendToTarget(conn gnet.Conn, msgType uint32, body []byte) (err error) {
	bodyLen := len(body)
	if msgType < 10000 {
		// 业务相关的消息类型目前都是大于100000的
		//这一类属于基础功能号， 与分区号等无关
		buf := make([]byte, 4 + 4 + bodyLen + 4)
		//消息头
		binary.BigEndian.PutUint32(buf, msgType)
		//消息体长度
		binary.BigEndian.PutUint32(buf[4:8], uint32(bodyLen))
		// 消息体
		copy(buf[8:8+bodyLen], body)
		// 消息尾
		checkSum := uint32(0)
		for _, b := range buf {
			checkSum = checkSum + uint32(b)
		}
		checkSum = checkSum % 256
		binary.BigEndian.PutUint32(buf[8+bodyLen:], checkSum)
		es.logfile.Infof("outgoing: target=%s, data=%v", conn.RemoteAddr(), buf)
		if err = conn.AsyncWrite(buf); err != nil {
			es.logfile.Errorf("response send failed: %s", err.Error())
			return err
		}
	} else {
		// 随机用一个分区发出去
		partitionNo := es.partitionNos[rand.Intn(len(es.partitionNos))]
		// 业务相关消息类型，与分区有关，与基础功能号相比，多了分区号和回报序号
		buf := make([]byte, 4 + 4 + 4 + 8 + bodyLen + 4)
		//消息头
		binary.BigEndian.PutUint32(buf, msgType)
		//消息体长度, 分区号和回报序号也需要包含到消息体
		binary.BigEndian.PutUint32(buf[4:8], uint32(bodyLen + 4 + 8))
		// 消息体
		binary.BigEndian.PutUint32(buf[8:12], partitionNo)
		sequence := es.incrSequence(es.platformID, partitionNo)
		binary.BigEndian.PutUint64(buf[12:20], sequence)
		copy(buf[20:20+bodyLen], body)
		// 消息尾
		checkSum := uint32(0)
		for _, b := range buf {
			checkSum = checkSum + uint32(b)
		}
		checkSum = checkSum % 256
		binary.BigEndian.PutUint32(buf[20+bodyLen:], checkSum)
		es.logfile.Infof("outgoing: target=%s, data=%v", conn.RemoteAddr(), buf)
		if err = conn.AsyncWrite(buf); err != nil {
			es.logfile.Errorf("response send failed: %s", err.Error())
			//发送失败也需要继续保存到本地文件，因此这里不能return
		}
		var lock sync.Mutex
		lock.Lock()
		defer lock.Unlock()
		es.saveMessage(partitionNo, sequence, buf)
	}
	return nil
}

func (es *SzBinaryServer) AsyncSend(msgType uint32, body []byte) (err error) {
	return es.sendToTarget(es.client, msgType, body)
}

func (es *SzBinaryServer) asyncSendToTarget(c gnet.Conn, msgType uint32, body []byte) (err error) {
	return es.sendToTarget(c, msgType, body)
}

func (es *SzBinaryServer) incrSequence(platformID uint16, partitionNo uint32) uint64 {
	key := fmt.Sprintf("%d_%d", platformID, partitionNo)
	if value, ok := es.sequence[key]; ok {
		// key 存在，则直接累加1返回去
		return seq.AddUint64(value, 1)
	}
	// key不存在，则返回1
	var lock sync.Mutex
	lock.Lock()
	defer lock.Unlock()
	var initValue uint64 = 1
	es.sequence[key] = &initValue
	return initValue
}

func (es *SzBinaryServer) Run() (err error) {
	if es.running.Load() {
		return errIsRunning
	}
	if len(es.partitionNos) == 0 {
		return errEmptyPartitions
	}
	es.logfile, es.logFlush, err = logging.CreateLoggerAsLocalFile(fmt.Sprintf("./logs/%s-%s.log", es.senderCompID, es.targetCompID), zapcore.DebugLevel)
	if err != nil {
		return
	}
	// load sequence
	if es.sequenceFile, err = openOrCreateFile(es.sequenceFileName, 0660); err != nil {
		return
	}
	if err = es.loadSequence(); err != nil {
		return
	}
	// load index
	if es.indexFile, err = openOrCreateFile(es.indexFileName, 0660); err != nil {
		return
	}
	if err = es.loadIndex(); err != nil {
		return
	}
	// datafile
	if es.dataFile, err = openOrCreateFile(es.dataFileName, 0660); err != nil {
		return
	}

	go func() {
		err = gnet.Serve(es, fmt.Sprintf("tcp://:%d", es.port), gnet.WithMulticore(true), gnet.WithCodec(newSzBinaryCodec()), gnet.WithLogger(es.logfile), gnet.WithTicker(true))
		es.runningChan <- true
	}()
	<- es.runningChan
	if err == nil {
		if es.pool == nil {
			es.pool = goroutine.Default()
		}
		es.running.Store(true)
	}
	return err
}

func (es *SzBinaryServer) saveMessage(partitionNo uint32, reportIndex uint64, data []byte) (err error) {
	var lock sync.Mutex
	lock.Lock()
	defer lock.Unlock()
	//保存data
	offset, err := es.dataFile.Seek(0, os.SEEK_END)
	if err != nil {
		es.logfile.Errorf("unable to seek to end of file: %s: %s", es.dataFileName, err.Error())
		return
	}
	if _,err = es.dataFile.Write(data); err != nil {
		es.logfile.Errorf("unable to write data file: %s: %s", es.dataFileName, err.Error())
		return
	}
	// 保存内存索引
	es.indexCache[fmt.Sprintf("%d_%d_%d", es.platformID, partitionNo, reportIndex)] = msgDef{
		Offset: offset,
		Size:   len(data),
	}
	// 保存索引
	if _, err = fmt.Fprintf(es.indexFile, "%d_%d_%d,%d,%d\n", es.platformID, partitionNo, reportIndex, offset, len(data)); err != nil {
		es.logfile.Errorf("unable to write index file: %s: %s", es.indexFileName, err.Error())
		return
	}
	// 保存sequence
	es.sequenceFile.Truncate(0)
	es.sequenceFile.Seek(0, os.SEEK_SET)
	for k,v := range es.sequence {
		if _, err = fmt.Fprintf(es.sequenceFile, "%s,%d\n", k, *v); err != nil {
			es.logfile.Errorf("unable to write sequence file: %s: %s", es.sequenceFileName, err.Error())
			return
		}
	}
	return nil
}

func (es *SzBinaryServer) BusinessReject(applID, pbuID, securityID, SecurityIDSource string, refSeqNum uint64, refMsgType uint32, rejectRefID string, rejectReason uint16, rejectText string) (err error) {
	bodyLen := 3 + 8 + 6 + 8 + 4 + 8 + 4 + 10 + 2 + 50
	//消息体
	b := make([]byte, bodyLen)
	copy(b[0:3], Str2bytes(applID))
	binary.BigEndian.PutUint64(b[3:11], LocalTimeStamp())
	copy(b[11:17], Str2bytes(pbuID))
	copy(b[17:25], Str2bytes(securityID))
	copy(b[25:29], Str2bytes(SecurityIDSource))
	binary.BigEndian.PutUint64(b[29:37], refSeqNum)
	binary.BigEndian.PutUint32(b[37:41], refMsgType)
	copy(b[41:51], Str2bytes(rejectRefID))
	binary.BigEndian.PutUint16(b[51:53], rejectReason)
	copy(b[53:103], Str2bytes(rejectText))
	return es.AsyncSend(msgtypeBusinessReject, b)
}

func (es *SzBinaryServer) Stop() (err error) {
	if !es.running.Load() {
		return errIsNotRunning
	}
	err = gnet.Stop(context.Background(), fmt.Sprintf("tcp://:%d", es.port))
	if err == nil {
		es.running.Store(false)
		if es.pool != nil {
			es.pool.Release()
			es.pool = nil
		}
	}
	return
}

func (es *SzBinaryServer) loadSequence() error {
	for {
		var platformID uint16
		var partionNo uint32
		var reportIndex uint64
		cnt, err := fmt.Fscanf(es.sequenceFile, "%d_%d,%d\n", &platformID, &partionNo, &reportIndex)
		if err == io.EOF || cnt != 3 {
			break
		}
		if err != nil {
			return err
		}
		es.sequence[fmt.Sprintf("%d_%d", platformID, partionNo)] = &reportIndex
	}
	for k, v := range es.sequence {
		es.logfile.Infof("平台编号=%s, 分区号=%s, 当前起始序号=%d", strings.Split(k,"_")[0], strings.Split(k,"_")[1], *v)
	}
	return nil
}

func (es *SzBinaryServer) loadIndex() error {
	for {
		var offset int64
		var size int
		var platformID uint16
		var partionNo uint32
		var reportIndex uint64
		cnt, err := fmt.Fscanf(es.indexFile, "%d_%d_%d,%d,%d\n", &platformID, &partionNo, &reportIndex, &offset, &size)
		if err == io.EOF || cnt != 5 {
			break
		}
		if err != nil {
			return err
		}
		es.indexCache[fmt.Sprintf("%d_%d_%d", platformID, partionNo, reportIndex)] = msgDef{
			Offset: offset,
			Size:   size,
		}
	}
	return nil
}

func (es *SzBinaryServer) getMessage() error {
	raw, err := ioutil.ReadFile(es.indexFileName)
	if err != nil{
		if os.IsNotExist(err) {
			es.logfile.Infof("索引信息文件信息不存在，索引信息重新开始计数")
			return nil
		}
		es.logfile.Errorf("加载索引信息文件失败：%s", err.Error())
		return err
	}
	if len(raw) > 0 {
		buffer := bytes.NewBuffer(raw)
		dec := gob.NewDecoder(buffer)
		if err = dec.Decode(&es.indexCache); err != nil{
			es.logfile.Errorf("解析索引信息文件失败：%s", err.Error())
			return err
		}
	}
	return nil
}

func NewBinaryServer(platformID uint16, port int, senderCompID, targetCompID string, partitionNos []uint32, onBusinessRequest func(msgtype uint32, body []byte)) *SzBinaryServer {
	return &SzBinaryServer{
		connected:    atomic.NewBool(false),
		logon:        atomic.NewBool(false),
		senderCompID: senderCompID,
		targetCompID: targetCompID,
		heartBeatInt: 5,
		pool:         goroutine.Default(),
		onBusinessRequestCallback: onBusinessRequest,
		running: atomic.NewBool(false),
		runningChan: make(chan bool),
		port: port,
		platformID: platformID,
		partitionNos: partitionNos,
		sequence: make(map[string]*uint64),
		indexCache: make(map[string]msgDef),
		sequenceFileName: fmt.Sprintf("./store/%s-%s.%s.seqnums", senderCompID, targetCompID, time.Now().Format("20060102")),
		indexFileName: fmt.Sprintf("./store/%s-%s.%s.index", senderCompID, targetCompID, time.Now().Format("20060102")),
		dataFileName: fmt.Sprintf("./store/%s-%s.%s.data", senderCompID, targetCompID, time.Now().Format("20060102")),
	}
}