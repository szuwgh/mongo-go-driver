package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"gitee.com/Trisia/gotlcp/tlcp"
	"github.com/emmansun/gmsm/smx509"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	// 1. 加载服务端根 CA
	pool := smx509.NewCertPool()
	pemData, err := os.ReadFile("/root/unvdb_cert/cacert.pem")
	if err != nil {
		panic(err)
	}
	ca, err := smx509.ParseCertificatePEM(pemData)
	if err != nil {
		panic(err)
	}
	pool.AddCert(ca)

	// 2. 加载客户端签名证书（用于认证身份）
	authCert, err := tlcp.LoadX509KeyPair("/root/unvdb_cert/client/client.crt", "/root/unvdb_cert/client/client.key")
	if err != nil {
		panic(err)
	}

	// 3. 加载客户端加密证书（用于 ECDHE 密钥交换）
	encCert, err := tlcp.LoadX509KeyPair("/root/unvdb_cert/client/client_enc.crt", "/root/unvdb_cert/client/client_enc.key")
	if err != nil {
		panic(err)
	}

	// 4. 构建 TLCP 配置，启用双向认证并指定密码套件顺序
	config := &tlcp.Config{
		RootCAs:      pool,
		Certificates: []tlcp.Certificate{authCert, encCert},
		CipherSuites: []uint16{
			tlcp.ECDHE_SM4_GCM_SM3,
			tlcp.ECDHE_SM4_CBC_SM3,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://unvdb:unvdb@192.168.4.134:27019").SetTLCPConfig(config))

	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("documentdb").Collection("patient")

	cursor, err := coll.Find(ctx, bson.D{}) // 空过滤器抓取所有文档
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		log.Fatal(err)
	}

	for _, doc := range docs {
		fmt.Println(doc)
	}
}
