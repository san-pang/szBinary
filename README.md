# szBinary
basic binary framework for ShenZhen exchange深交所第五代binary基础通信框架

# Installation
```
go get github.com/san-pang/szBinary
```

# Example:  
```
import (
  "github.com/san-pang/szBinary"
  "go.uber.org/zap/zapcore"
  "log"
)

func main()  {
  server, err := szBinary.NewBinaryServer(szBinary.PlatformID5, 9001, "sender", "target", []uint32{1,2,4}, onBusinessRequest, zapcore.InfoLevel)
  if err != nil {
    log.Fatalln(err)
  }
  defer server.Stop()
  if err = server.Run(); err != nil {
    log.Fatalln(err)
  }
  if server.Connected() {
    // 发送buff数据给客户端, buff打包目前需由用户自己实现
    server.AsyncSend(100414, []byte{})
    // 发送平台信息给客户端（实际上客户端登录后会自动发送该消息，无需用户自己发送，如有必要可以手工触发该消息发送）
    server.SendPlatformInfo()
    // 发送平台状态给客户端（实际上客户端登录后会自动发送平台开放消息，无需用户自己发送，如有必要可以手工触发该消息发送）
    server.SendPlatformState(szBinary.PlatformState_Close)
    // 发送当天的回报结束消息给客户端
    server.SendReportFinish(1, 666)
  }
}

func onBusinessRequest(msgtype uint32, body []byte)  {
  // 业务请求回调
  log.Println("msgtype=", msgtype, "body=", body)
}
```
