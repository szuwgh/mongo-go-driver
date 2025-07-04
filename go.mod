module go.mongodb.org/mongo-driver/v2

go 1.19

require (
	gitee.com/Trisia/gotlcp v1.4.1
	github.com/davecgh/go-spew v1.1.1
	github.com/emmansun/gmsm v0.29.7
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.6.0
	github.com/klauspost/compress v1.16.7
	github.com/xdg-go/scram v1.1.2
	github.com/xdg-go/stringprep v1.0.4
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78
	golang.org/x/crypto v0.33.0
	golang.org/x/sync v0.11.0
)

require (
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
)

replace golang.org/x/net/http2 => golang.org/x/net/http2 v0.23.0 // GODRIVER-3225
