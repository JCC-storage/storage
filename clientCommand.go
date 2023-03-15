package main

import (
    "context"
    "io"
    "os"
    "fmt"
    "path/filepath"
    "sync"
    "strconv"
    agentcaller "proto"

    //"github.com/pborman/uuid"
    //"github.com/streadway/amqp"

    "google.golang.org/grpc"

    _ "google.golang.org/grpc/balancer/grpclb"
)
const (
    port = ":5000"
    packetSizeInBytes=10
)

func Move(bucketName string, objectName string, destination string){
    //将bucketName, objectName, destination发给协调端，分析是否允许调度，若允许，则返回调度目的地ip、冗余策略、replication hash或blocks hash
    ip := "localhost"
    redundancy := "rep"
    hashs := []string{"rep.json0"}
    ids := []int{0,1,2,3,4}
    ecName := "ecname"
    
    switch redundancy {
        case "ec":
            ecMove(ip, hashs, ids, ecName)
        case "rep":
            repMove(ip, hashs[0])
    }

}

func RepRead(localFilePath string, bucketName string, objectName string){
    fmt.Println("read "+bucketName+"/"+objectName+" to "+localFilePath)
    //获取块hash，ip，序号，编码参数等

    repHash := "rep.json0"
    ip := "localhost" 
    var fileSizeInBytes int64=41

    numPacket := (fileSizeInBytes+packetSizeInBytes-1)/(packetSizeInBytes)
    fmt.Println(numPacket)
    //rpc相关
    conn, err := grpc.Dial(ip+port, grpc.WithInsecure())
    if err != nil {
        panic(err)
    }
    client := agentcaller.NewTranBlockOrReplicaClient(conn) 

    fDir, err := os.Executable()
    if err != nil {
        panic(err)
    }
    fURL := filepath.Join(filepath.Dir(fDir), "assets")
    _, err = os.Stat(fURL)
    if os.IsNotExist(err) {
        os.MkdirAll(fURL, os.ModePerm)
    }

    file, err := os.Create(filepath.Join(fURL, localFilePath))
    if err != nil {
        return
    }
    
    stream, _ := client.GetBlockOrReplica(context.Background(), &agentcaller.GetReq{
		BlockOrReplicaHash: repHash,
	})
    fmt.Println(numPacket)
	for i:=0;int64(i)<numPacket;i++{
        fmt.Println(i)
		res, _:= stream.Recv()
	    fmt.Println(res.BlockOrReplicaData)
        file.Write(res.BlockOrReplicaData)
	}
    file.Close() 
    conn.Close()   
}

func RepWrite(localFilePath string, bucketName string, objectName string, numRep int){
    fmt.Println("write "+localFilePath+" as "+bucketName+"/"+objectName)
    //获取文件大小
    fileInfo,_ := os.Stat(localFilePath)
    fileSizeInBytes := fileInfo.Size()
    fmt.Println(fileSizeInBytes)

    //计算每个块的packet数
    numWholePacket := fileSizeInBytes/packetSizeInBytes
    lastPacketInBytes:=fileSizeInBytes%packetSizeInBytes
    numPacket:=numWholePacket
    if lastPacketInBytes>0 {
        numPacket++
    }
    
    //发送写请求，分配写入节点
    ips:=make([]string, numRep) 

    for i:=0;i<numRep;i++ {
        ips[i]="localhost"
    }

    //创建channel
    loadDistributeBufs:=make([]chan []byte,numRep) 

    for i := 0; i < numRep; i++ {
        loadDistributeBufs[i] = make(chan []byte)
    }
    
    //正式开始写入
    go loadDistribute(localFilePath, loadDistributeBufs[:], numWholePacket, lastPacketInBytes)//从本地文件系统加载数据
    wg := sync.WaitGroup{}
    wg.Add(numRep)
    for i:=0;i<numRep;i++ {
        go send("rep.json"+strconv.Itoa(i), ips[i], loadDistributeBufs[i], numPacket, &wg)//"block1.json"这样参数不需要
    }
    wg.Wait()
}

func EcRead(localFilePath string, bucketName string, objectName string){
    fmt.Println("read "+bucketName+"/"+objectName+" to "+localFilePath)
    //获取块hash，ip，序号，编码参数等
    const ecK int = 2
    const ecN int = 3
    var coefs = [][]int64 {{1,1,1},{1,2,3}}//2应替换为ecK，3应替换为ecN
    var blockHashs = []string{"block1.json","block2.json"}
    var ips  = []string {"localhost","localhost"} 
    var fileSizeInBytes int64=41
    numPacket := (fileSizeInBytes+int64(ecK)*packetSizeInBytes-1)/(int64(ecK)*packetSizeInBytes)
    fmt.Println(numPacket)
    //创建channel
    var getBufs [ecK]chan []byte
    var decodeBufs [ecK]chan []byte
    for i := 0; i < ecK; i++ {
        getBufs[i] = make(chan []byte)
    }
    for i := 0; i < ecK; i++ {
        decodeBufs[i] = make(chan []byte)
    }
    
    
    wg := sync.WaitGroup{}
    wg.Add(1)
    go get(blockHashs[0], ips[0], getBufs[0], numPacket)
    go get(blockHashs[1], ips[1], getBufs[1], numPacket)
    go encode(getBufs[:], decodeBufs[:], coefs, numPacket)
    go persist(decodeBufs[:], numPacket, localFilePath, &wg)
    wg.Wait()
    
}

func EcWrite(localFilePath string, bucketName string, objectName string, ecName string){
    fmt.Println("write "+localFilePath+" as "+bucketName+"/"+objectName)
    //获取文件大小
    fileInfo,_ := os.Stat(localFilePath)
    fileSizeInBytes := fileInfo.Size()
    fmt.Println(fileSizeInBytes)
    //调用纠删码库，获取编码参数及生成矩阵
    const ecK int = 2
    const ecN int = 3
    var coefs = [][]int64 {{1,1,1},{1,2,3}}//2应替换为ecK，3应替换为ecN

    //计算每个块的packet数
    numPacket := (fileSizeInBytes+int64(ecK)*packetSizeInBytes-1)/(int64(ecK)*packetSizeInBytes)
    fmt.Println(numPacket)
    
    //发送写请求，分配写入节点
    var ips  = []string {"localhost","localhost","localhost"} //3应替换为ecN

    //创建channel
    var loadBufs [ecK]chan []byte
    var encodeBufs [ecN]chan []byte

    for i := 0; i < ecK; i++ {
        loadBufs[i] = make(chan []byte)
    }
    
    for i := 0; i < ecN; i++ {
        encodeBufs[i] = make(chan []byte)
    }

    //正式开始写入
    go load(localFilePath, loadBufs[:], numPacket*int64(ecK), fileSizeInBytes)//从本地文件系统加载数据
    go encode(loadBufs[:], encodeBufs[:], coefs, numPacket)
    wg := sync.WaitGroup{}
    wg.Add(3)
    go send("block1.json", ips[0], encodeBufs[0], numPacket, &wg)//"block1.json"这样参数不需要
    go send("block2.json", ips[1], encodeBufs[1], numPacket, &wg)
    go send("block3.json", ips[2], encodeBufs[2], numPacket, &wg)
    wg.Wait()
}

func repMove(ip string, hash string){
    //通过消息队列发送调度命令
}

func ecMove(ip string, hashs []string, ids []int, ecName string){
    //通过消息队列发送调度命令
}

func loadDistribute(localFilePath string, loadDistributeBufs []chan []byte, numWholePacket int64, lastPacketInBytes int64){
    fmt.Println("loadDistribute "+ localFilePath)
    file, _ := os.Open(localFilePath)
    for i:=0;int64(i)<numWholePacket;i++ {
        buf := make([]byte, packetSizeInBytes)
        _, err := file.Read(buf)
        if err != nil && err != io.EOF {
            break
        }
        for j:=0;j<len(loadDistributeBufs);j++ {
            loadDistributeBufs[j]<-buf
        }
    }
    if lastPacketInBytes>0 {
        buf := make([]byte, lastPacketInBytes)
        file.Read(buf)
        for j:=0;j<len(loadDistributeBufs);j++ {
            loadDistributeBufs[j]<-buf
        }
    }
    fmt.Println("load over")
    for i:=0;i<len(loadDistributeBufs);i++{
        close(loadDistributeBufs[i])
    }
    file.Close()
}

func load(localFilePath string, loadBufs []chan []byte, totalNumPacket int64, fileSizeInBytes int64){
    fmt.Println("load "+ localFilePath)
    file, _ := os.Open(localFilePath)
    for i:=0;int64(i)<totalNumPacket;i++ {
        buf := make([]byte, packetSizeInBytes)
        _, err := file.Read(buf)
        if err != nil && err != io.EOF {
            break
        }
        idx:=i%len(loadBufs)
        loadBufs[idx]<-buf
    }
    fmt.Println("load over")
    for i:=0;i<len(loadBufs);i++{
        close(loadBufs[i])
    }
    file.Close()
}

func encode(inBufs []chan []byte, outBufs []chan []byte, coefs [][]int64, numPacket int64){
    fmt.Println("encode ")
    var tmpIn [][]byte
    tmpIn = make([][]byte, len(inBufs))
    
    for i := 0; int64(i) < numPacket; i++ {
        for j :=0; j < len(inBufs); j++ {//2
            tmpIn[j]=<-inBufs[j]
            //fmt.Println(tmpIn[j])

        }    
        for j := 0; j < len(outBufs); j++{
            tmp := make([]byte, packetSizeInBytes)
            for k := 0; k < packetSizeInBytes; k++{
                for w := 0; w < len(inBufs); w++{
                    //示意，需要调用纠删码编解码引擎：  tmp[k] = tmp[k]+(tmpIn[w][k]*coefs[w][j])  
                    /*fmt.Println(w)
                    fmt.Println(k)
                    fmt.Println(i)
                    fmt.Println(tmpIn[w])
                    fmt.Println(tmpIn[w][k])
                    fmt.Println("-----")*/
                    tmp[k] = tmp[k]+tmpIn[w][k] 
                }
            }
            outBufs[j]<-tmp
         }
    }
    fmt.Println("encode over")
    for i:=0;i<len(outBufs);i++{
        close(outBufs[i])
    }
}

func send(blockhash string, ip string, inBuf chan []byte, numPacket int64, wg *sync.WaitGroup){
    fmt.Println("send "+blockhash)
    //rpc相关
    conn, err := grpc.Dial(ip+port, grpc.WithInsecure())
    if err != nil {
        panic(err)
    }
       
    client := agentcaller.NewTranBlockOrReplicaClient(conn)

    stream, err := client.SendBlockOrReplica(context.Background())
    if err != nil {
        panic(err)
    }
    for i:=0;int64(i)<numPacket;i++{
        buf:=<-inBuf
        fmt.Println(buf)
        err:=stream.Send(&agentcaller.BlockOrReplica{
            BlockOrReplicaName: blockhash,
            BlockOrReplicaHash: blockhash,
            BlockOrReplicaData: buf,
        })
        if err != nil && err != io.EOF{
            panic(err)
        }
	}
    res, err := stream.CloseAndRecv()
    fmt.Println(res)
    wg.Done()
    conn.Close()
    return
}

func get(blockHash string, ip string, getBuf chan []byte, numPacket int64){
    //rpc相关
    conn, err := grpc.Dial(ip+port, grpc.WithInsecure())
    if err != nil {
        panic(err)
    }
    client := agentcaller.NewTranBlockOrReplicaClient(conn) 
    //rpc get
    stream, _ := client.GetBlockOrReplica(context.Background(), &agentcaller.GetReq{
		BlockOrReplicaHash: blockHash,
	})
    fmt.Println(numPacket)
	for i:=0;int64(i)<numPacket;i++{
        fmt.Println(i)
		res, _:= stream.Recv()
	    fmt.Println(res.BlockOrReplicaData)
        getBuf<-res.BlockOrReplicaData
	}
    close(getBuf)
    conn.Close()
}


func persist(inBuf []chan []byte, numPacket int64, localFilePath string, wg *sync.WaitGroup){
    fDir, err := os.Executable()
    if err != nil {
        panic(err)
    }
    fURL := filepath.Join(filepath.Dir(fDir), "assets")
    _, err = os.Stat(fURL)
    if os.IsNotExist(err) {
        os.MkdirAll(fURL, os.ModePerm)
    }

    file, err := os.Create(filepath.Join(fURL, localFilePath))
    if err != nil {
        return
    }
    
    for i := 0; int64(i) < numPacket; i++ {
        for j := 0; j < len(inBuf); j++{
            tmp:=<-inBuf[j]
            fmt.Println(tmp)
            file.Write(tmp)
        }
    }
    file.Close()
    wg.Done()
}