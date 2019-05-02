package main

import (
	"flag"

	"github.com/spf13/viper"
)

const bucketName = "couchbase.bucket"
const username = "couchbase.username"
const password = "couchbase.password"
const connStr = "couchbase.connString"

type Options struct {
	ConnStr    string
	Username   string
	Password   string
	BucketName string
}

func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
}

func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.ConnStr = v.GetString(connStr)
	opt.Username = v.GetString(username)
	opt.Password = v.GetString(password)
	opt.BucketName = v.GetString(bucketName)
}
