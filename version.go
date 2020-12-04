package main

import (
	"flag"
	"fmt"
)

const AppVersion = "v0.2.0"

type versionCmd struct{}

func (cmd *versionCmd) run(args []string) error {
	fmt.Println("hkt version ", AppVersion)
	return nil
}

func (cmd *versionCmd) addFlags(*flag.FlagSet) {

}

func (cmd *versionCmd) environFlags() map[string]string {
	return map[string]string{}
}
