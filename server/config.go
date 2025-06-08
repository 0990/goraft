package server

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	Servers []struct {
		Ip   string
		Port int
	} `yaml:"servers"`
	Me int `yaml:"me"`
}

func parseCfg(path string) (Config, error) {
	// 读取文件
	cfgFile, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	config := Config{}
	// 使用外部库解析yaml文件
	err = yaml.Unmarshal(cfgFile, &config)
	if err != nil {
		return Config{}, err
	}

	return config, nil
}
