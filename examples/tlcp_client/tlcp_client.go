package main

import (
	"fmt"
	"io"
	"os"

	"gitee.com/Trisia/gotlcp/tlcp"
	"github.com/emmansun/gmsm/smx509"
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

	// 5. 发起连接
	conn, err := tlcp.Dial("tcp", "192.168.4.134:27019", config)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Connected via TLCP! Send a message…")
	_, _ = conn.Write([]byte("Hello server\n"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		panic(err)
	}
	fmt.Printf("Server says: %s\n", buf[:n])
}
