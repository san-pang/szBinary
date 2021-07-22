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
  "log"
)

func main()  {
  server, err := szBinary.NewBinaryServer(szBinary.PlatformID5, 9001, "sender", "target", []uint32{1}, onBusinessRequest, onReportSynchronization)
  if err != nil {
    log.Fatalln(err)
  }
  defer server.Release()
  if err = server.Run(); err != nil {
    log.Fatalln(err)
  }

  if server.Connected() {
    // 发送buff数据给客户端
    server.AsyncSend([]byte{})
    // 发送平台信息给客户端（实际上客户端登录后会自动发送该消息，无需额外发送，如有必要可以触发该消息发送）
    server.SendPlatformInfo()
    // 发送平台状态给客户端（实际上客户端登录后会自动发送平台开放消息，无需额外发送，如有必要可以触发该消息发送）
    server.SendPlatformState(szBinary.PlatformState_Close)
    // 发送当天的回报结束消息给客户端
    server.SendReportFinish(1, 666)
  }
}

func onBusinessRequest(msgtype uint32, data []byte)  {
  // 业务请求回调
  log.Println("msgtype=", msgtype, "data=", data)
}

func onReportSynchronization(syncParams szBinary.ReportSynchronization)  {
  // 回报同步请求回调
  log.Printf("receive report sync request: %+v", syncParams)
}
```
