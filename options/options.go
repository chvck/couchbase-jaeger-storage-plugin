package options

import (
	"flag"

	"github.com/spf13/viper"
)

const bucketName = "couchbase.bucket"
const username = "couchbase.username"
const password = "couchbase.password"
const connStr = "couchbase.connString"
const useAnalytics = "couchbase.useAnalytics"
const n1qlFallback = "couchbase.n1qlFallback"
const autoSetup = "couchbase.autoSetup"

type Options struct {
	ConnStr         string
	Username        string
	Password        string
	BucketName      string
	UseAnalytics    bool
	UseN1QLFallback bool
	AutoSetup       bool
}

func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
}

func (opt *Options) InitFromViper(v *viper.Viper) {
	v.SetDefault(bucketName, "default")
	v.SetDefault(connStr, "couchbase://localhost")
	v.SetDefault(useAnalytics, true)
	v.SetDefault(n1qlFallback, true)

	opt.ConnStr = v.GetString(connStr)
	opt.Username = v.GetString(username)
	opt.Password = v.GetString(password)
	opt.BucketName = v.GetString(bucketName)
	opt.UseAnalytics = v.GetBool(useAnalytics)
	opt.UseN1QLFallback = v.GetBool(n1qlFallback)
	opt.AutoSetup = v.GetBool(autoSetup)
}
