module gitlink.org.cn/cloudream/storage

go 1.21

toolchain go1.23.2

replace gitlink.org.cn/cloudream/common v0.0.0 => ../common

require (
	github.com/aliyun/aliyun-oss-go-sdk v3.0.2+incompatible
	github.com/aws/aws-sdk-go-v2 v1.32.6
	github.com/aws/aws-sdk-go-v2/credentials v1.17.47
	github.com/aws/aws-sdk-go-v2/service/s3 v1.71.0
	github.com/gin-gonic/gin v1.7.7
	github.com/go-sql-driver/mysql v1.7.1
	github.com/hashicorp/golang-lru/v2 v2.0.5
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.24.9+incompatible
	github.com/inhies/go-bytesize v0.0.0-20220417184213-4913239db9cf
	github.com/jedib0t/go-pretty/v6 v6.4.7
	github.com/klauspost/reedsolomon v1.11.8
	github.com/magefile/mage v1.15.0
	github.com/samber/lo v1.38.1
	github.com/smartystreets/goconvey v1.8.1
	github.com/spf13/cobra v1.8.0
	github.com/tencentyun/cos-go-sdk-v5 v0.7.56
	gitlink.org.cn/cloudream/common v0.0.0
	google.golang.org/grpc v1.57.0
	google.golang.org/protobuf v1.31.0
	gorm.io/gorm v1.25.7
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.6 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mozillazg/go-httpheader v0.2.1 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	golang.org/x/time v0.7.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	github.com/antonfisher/nested-logrus-formatter v1.3.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-playground/locales v0.13.0 // indirect
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/go-playground/validator/v10 v10.8.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/imdario/mergo v0.3.15 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/sirupsen/logrus v1.9.2
	github.com/smarty/assertions v1.15.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/streadway/amqp v1.1.0 // indirect
	github.com/ugorji/go/codec v1.2.11 // indirect
	github.com/zyedidia/generic v1.2.1 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.9 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/crypto v0.9.0 // indirect
	golang.org/x/exp v0.0.0-20230519143937-03e91628a987 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20230526161137-0005af68ea54 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230525234035-dd9d682886f9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230525234030-28d5490b6b19 // indirect
	gorm.io/driver/mysql v1.5.7
)
