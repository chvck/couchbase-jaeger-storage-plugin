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
	ConnStr    string `yaml:"connString"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	BucketName string `yaml:"bucket"`
}

func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
}

func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.ConnStr = v.GetString(connStr)
	opt.Username = v.GetString(username)
	opt.Password = v.GetString(password)
	opt.BucketName = v.GetString(bucketName)
}
