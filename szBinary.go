package szBinary

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/logging"
	"github.com/panjf2000/gnet/pool/goroutine"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const msgtypeLogon uint32 = 1
const msgtypeLogout uint32 = 2
const msgtypeHeartbeat uint32 = 3
const msgtypeBusinessReject uint32 = 4
const msgtypeReportSynchronization uint32 = 5
const msgtypePlatformState uint32 = 6
const msgtypeReportFinished uint32 = 7
const msgtypePlatformInfo uint32 = 9

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
	onBusinessRequestCallback func(msgtype uint32, data []byte)
	onReportSynchronizationCallback func(syncParams ReportSynchronization)
	running *atomic.Bool
	runningChan chan bool
	port int
	platformID uint16
	partitionNos []uint32
}

func (es *SzBinaryServer) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	logger.Infof("server is listening on %s", srv.Addr.String())
	es.runningChan <- true
	return
}

func (es *SzBinaryServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	logger.Infof("%s disconnected!", c.RemoteAddr().String())
	es.client = nil
	es.logon.Store(false)
	es.connected.Store(false)
	return
}

func (es *SzBinaryServer) OnShutdown(svr gnet.Server) {
	logger.Infof("%s is going to shutdown", svr.Addr.String())
	loggerFlush()
}

func (es *SzBinaryServer) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logger.Infof("%s is going to connect", c.RemoteAddr().String())
	if es.connected.Load() {
		// already connected, only one client allowed to be connected
		out = logOut("another client already connected")
		action = gnet.Close
		return
	}
	// connect
	es.connected.Store(true)
	es.client = c
	es.sendTime = atomic.NewTime(time.Now())
	es.receiveTime = atomic.NewTime(time.Now())
	logger.Infof("%s connected!", c.RemoteAddr().String())
	return
}

func (es *SzBinaryServer) SendPlatformState(status uint16) (err error) {
	// 本消息用于新一代交易系统向 OMS 提供各平台的当前状态，便于 OMS 控制是否发送委托
	bodyLen := 2 + 2
	//消息头 + 消息体
	b := make([]byte, 4 + 4 + bodyLen)
	//消息头
	binary.BigEndian.PutUint32(b, msgtypePlatformState)
	//消息体长度
	binary.BigEndian.PutUint32(b[4:8], uint32(bodyLen))
	//消息体
	binary.BigEndian.PutUint16(b[8:10], es.platformID)
	binary.BigEndian.PutUint16(b[10:12], status)
	return es.AsyncSend(b)
}

func (es *SzBinaryServer) SendReportFinish(partitionNo uint32, reportIndex uint64) (err error) {
	//TGW 使用该消息通知 OMS 本交易日当前平台对应分区回报发送完毕
	bodyLen := 4 + 8 + 2
	//消息头 + 消息体
	b := make([]byte, 4 + 4 + bodyLen)
	//消息头
	binary.BigEndian.PutUint32(b, msgtypeReportFinished)
	//消息体长度
	binary.BigEndian.PutUint32(b[4:8], uint32(bodyLen))
	//消息体
	binary.BigEndian.PutUint32(b[8:12], partitionNo)
	binary.BigEndian.PutUint64(b[12:20], reportIndex)
	binary.BigEndian.PutUint16(b[20:22], es.platformID)
	return es.AsyncSend(b)
}

func (es *SzBinaryServer) SendPlatformInfo() (err error) {
	// 本消息用于 TGW 向 OMS 提供各平台的基本信息
	bodyLen := 2 + 4 + 4 * len(es.partitionNos)
	//消息头 + 消息体
	b := make([]byte, 4 + 4 + bodyLen)
	//消息头
	binary.BigEndian.PutUint32(b, msgtypePlatformInfo)
	//消息体长度
	binary.BigEndian.PutUint32(b[4:8], uint32(bodyLen))
	//消息体
	binary.BigEndian.PutUint16(b[8:10], es.platformID)
	binary.BigEndian.PutUint32(b[10:14], uint32(len(es.partitionNos)))
	for i:=0; i<len(es.partitionNos); i++ {
		binary.BigEndian.PutUint32(b[14+i*4:14+i*4+4], es.partitionNos[i])
	}
	return es.AsyncSend(b)
}

func (es *SzBinaryServer) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	es.pool.Submit(func() {
		es.receiveTime.Store(time.Now())
		msgType := binary.BigEndian.Uint32(frame[:4])
		bodyLen := binary.BigEndian.Uint32(frame[4:8])
		bodyBuff := frame[8:8+bodyLen]
		switch msgType {
		case msgtypeLogon:
			if !es.logon.Load() {
				senderCompID := bytes2str(bodyBuff[:20])
				targetCompID := bytes2str(bodyBuff[20:40])
				if es.senderCompID != strings.TrimSpace(targetCompID) || es.targetCompID != strings.TrimSpace(senderCompID) {
					es.AsyncSend(logOut("senderCompID and targetCompID not match"))
					action = gnet.Close
					return
				}
				HeartBtInt := binary.BigEndian.Uint32(bodyBuff[40:44])
				Password := bytes2str(bodyBuff[44:60])
				DefaultApplVerID := bytes2str(bodyBuff[60:92])
				es.AsyncSend(logOn(es.senderCompID, es.targetCompID, HeartBtInt, Password, DefaultApplVerID))
				es.heartBeatInt = HeartBtInt
				es.logon.Store(true)
				// send platform status after logon successfully
				es.SendPlatformState(PlatformState_Open)
				// send platform info
				es.SendPlatformInfo()
				return
			}
			// duplicate logon
			es.AsyncSend(logOut("duplicate logon"))
			return
		case msgtypeLogout:
			// not logon yet
			if !es.logon.Load() {
				es.AsyncSend(logOut("not logon yet, please logon first"))
				action = gnet.Close
				return
			}
			es.AsyncSend(logOut(""))
			action = gnet.Close
			return
		case msgtypeHeartbeat:
			// not logon yet
			if !es.logon.Load() {
				es.AsyncSend(logOut("not logon yet, please logon first"))
				action = gnet.Close
				return
			}
			return
		case msgtypeReportSynchronization:
			// not logon yet
			if !es.logon.Load() {
				es.AsyncSend(logOut("not logon yet, please logon first"))
				action = gnet.Close
				return
			}
			R := ReportSynchronization{}
			R.NoPartitions = binary.BigEndian.Uint32(bodyBuff[:4])
			for i:=uint32(0);i<R.NoPartitions;i++{
				partitionSync := new(PartitionSync)
				partitionSync.PartitionNo = binary.BigEndian.Uint32(bodyBuff[4 + 12*i:8 + 12*i])
				partitionSync.ReportIndex = binary.BigEndian.Uint64(bodyBuff[8 + 12*i:16 + 12*i])
				R.PartitionSyncs = append(R.PartitionSyncs, partitionSync)
			}
			es.onReportSynchronizationCallback(R)
		default:
			// not logon yet
			if !es.logon.Load() {
				es.AsyncSend(logOut("not logon yet, please logon first"))
				action = gnet.Close
				return
			}
			es.onBusinessRequestCallback(msgType, bodyBuff)
		}
	})
	return
}

func (es *SzBinaryServer) Connected() bool {
	return es.connected.Load() && es.client != nil
}

func (es *SzBinaryServer) Tick() (delay time.Duration, action gnet.Action) {
	if es.Connected() {
		if time.Now().Sub(es.sendTime.Load()).Seconds() >= float64(es.heartBeatInt) {
			// need to send a heartbeat message to client
			es.AsyncSend(heartbeat())
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

func (es *SzBinaryServer) AsyncSend(data []byte) (err error) {
	if !es.Connected() {
		err = errNotConnected
	}
	if err == nil {
		err = es.client.AsyncWrite(data)
	}
	if err != nil {
		logger.Errorf("error happened when sending data: %s", err.Error())
	}
	return err
}

func (es *SzBinaryServer) Run() (err error) {
	if es.running.Load() {
		return errIsRunning
	}
	go func() {
		err = gnet.Serve(es, fmt.Sprintf("tcp://:%d", es.port), gnet.WithMulticore(true), gnet.WithCodec(NewSzBinaryCodec()), gnet.WithTicker(true))
		es.runningChan <- true
	}()
	<- es.runningChan
	if err == nil {
		es.running.Store(true)
	}
	return err
}

func (es *SzBinaryServer) BusinessReject(applID, pbuID, securityID, SecurityIDSource string, refSeqNum uint64, refMsgType uint32, rejectRefID string, rejectReason uint16, rejectText string) (err error) {
	bodyLen := 3 + 8 + 6 + 8 + 4 + 8 + 4 + 10 + 2 + 50
	//消息头 + 消息体
	b := make([]byte, 4 + 4 + bodyLen)
	//消息类型
	binary.BigEndian.PutUint32(b, msgtypeBusinessReject)
	//消息体长度
	binary.BigEndian.PutUint32(b[4:8], uint32(bodyLen))
	//消息体
	copy(b[8:11], str2bytes(applID))
	binary.BigEndian.PutUint64(b[11:19], LocalTimeStamp())
	copy(b[19:25], str2bytes(pbuID))
	copy(b[25:33], str2bytes(securityID))
	copy(b[33:37], str2bytes(SecurityIDSource))
	binary.BigEndian.PutUint64(b[37:45], refSeqNum)
	binary.BigEndian.PutUint32(b[45:49], refMsgType)
	copy(b[49:59], str2bytes(rejectRefID))
	binary.BigEndian.PutUint16(b[59:61], rejectReason)
	copy(b[61:111], str2bytes(rejectText))
	return es.AsyncSend(b)
}

func (es *SzBinaryServer) Stop() (err error) {
	if !es.running.Load() {
		return errIsNotRunning
	}
	err = gnet.Stop(context.Background(), fmt.Sprintf("tcp://:%d", es.port))
	if err == nil {
		es.running.Store(false)
	}
	return
}

func (es *SzBinaryServer) Release()  {
	if es.running.Load() {
		es.Stop()
	}
	es.pool.Release()
}

func NewBinaryServer(platformID uint16, port int, senderCompID, targetCompID string, partitionNos []uint32, onBusinessRequest func(msgtype uint32, data []byte), onReportSynchronization func(syncParams ReportSynchronization)) (*SzBinaryServer, error) {
	var err error
	logger, loggerFlush, err = logging.CreateLoggerAsLocalFile(fmt.Sprintf("../logs/io_szbinary_%s_%s_%d", senderCompID, targetCompID, platformID), zapcore.InfoLevel)
	if err != nil {
		return nil, err
	}
	return &SzBinaryServer{
		connected:    atomic.NewBool(false),
		logon:        atomic.NewBool(false),
		senderCompID: senderCompID,
		targetCompID: targetCompID,
		heartBeatInt: 5,
		pool:         goroutine.Default(),
		onBusinessRequestCallback: onBusinessRequest,
		onReportSynchronizationCallback: onReportSynchronization,
		running: atomic.NewBool(false),
		runningChan: make(chan bool),
		port: port,
		platformID: platformID,
		partitionNos: partitionNos,
	}, nil
}


func str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func heartbeat() []byte {
	// msgtype + bodyLen
	buff := make([]byte, 4 + 4)
	// msgtype
	binary.BigEndian.PutUint32(buff[:4], msgtypeHeartbeat)
	// bodylen
	binary.BigEndian.PutUint32(buff[4:8], 0)
	return buff
}

func logOn(senderCompID, targetCompID string, heartBtInt uint32, password, defaultApplVerID string) []byte {
	// msgtype + bodyLen + senderCompID + targetCompID + heartBtInt + password + defaultApplVerID
	buff := make([]byte, 4 + 4 + 20 + 20 + 4 + 16 + 32)
	// msgtype
	binary.BigEndian.PutUint32(buff[:4], msgtypeLogon)
	// bodylen
	binary.BigEndian.PutUint32(buff[4:8], 92)
	// senderCompID
	copy(buff[8:28], str2bytes(senderCompID))
	// targetCompID
	copy(buff[28:48], str2bytes(targetCompID))
	// heartBtInt
	binary.BigEndian.PutUint32(buff[48:52], heartBtInt)
	// password
	copy(buff[52:68], str2bytes(password))
	// defaultApplVerID
	copy(buff[68:100], str2bytes(defaultApplVerID))
	return buff
}

func logOut(text string) []byte {
	// msgtype + bodyLen + SessionStatus + text
	buff := make([]byte, 4 + 4 + 4 + 200)
	// msgtype
	binary.BigEndian.PutUint32(buff[:4], msgtypeLogout)
	// bodylen
	binary.BigEndian.PutUint32(buff[4:8], 204)
	// 101 means SessionStatus=others
	binary.BigEndian.PutUint32(buff[8:12], 101)
	// text
	textBuff := bytes.Repeat([]byte{' '}, 200)
	copy(textBuff, str2bytes(text))
	copy(buff[12:], textBuff)
	return buff
}

func LocalTimeStamp() (timestamp uint64) {
	timestamp,_ = strconv.ParseUint(strings.ReplaceAll(time.Now().Format("20060102150405.000"), ".", ""), 10, 64)
	return
}

func LocalDate() (date uint32) {
	d,_ := strconv.ParseUint(time.Now().Format("20060102"), 10, 32)
	date = uint32(d)
	return
}