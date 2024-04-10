package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"syscall"
)

func main() {
	flag.Parse()
	if *configFile == "" {
		fmt.Printf("Usage: %s -c {configFile}\n", os.Args[0])
		os.Exit(1)
	}
	var (
		err          error
		masterAddr   string
		downloadAddr string
		tarName      string
	)

	if !checkLibsExist() {
		if runtime.GOARCH == AMD64 {
			tarName = fmt.Sprintf("%s.tar.gz", TarNamePre)
		} else if runtime.GOARCH == ARM64 {
			tarName = fmt.Sprintf("%s_%s.tar.gz", TarNamePre, ARM64)
		} else {
			fmt.Printf("cpu arch %s not supported", runtime.GOARCH)
			os.Exit(1)
		}

		masterAddr, err = parseMasterAddr(*configFile)
		if err != nil {
			fmt.Printf("parseMasterAddr err: %v\n", err)
			os.Exit(1)
		}
		downloadAddr, err = getClientDownloadAddr(masterAddr)
		if err != nil {
			fmt.Printf("get downloadAddr from master err: %v\n", err)
			os.Exit(1)
		}
		if !prepareLibs(downloadAddr, tarName) {
			os.Exit(1)
		}
	}

	exeFile := os.Args[0]
	if err = moveFile(MainBinary, exeFile+".tmp"); err != nil {
		fmt.Printf("%v\n", err.Error())
		os.Exit(1)
	}
	if err = os.Rename(exeFile+".tmp", exeFile); err != nil {
		fmt.Printf("%v\n", err.Error())
		os.Exit(1)
	}

	execErr := syscall.Exec(exeFile, os.Args, os.Environ())
	if execErr != nil {
		fmt.Printf("exec %s %v error: %v\n", exeFile, os.Args, execErr)
		os.Exit(1)
	}
}

func checkLibsExist() bool {
	if _, err := os.Stat(MainBinary); err != nil {
		return false
	}
	if _, err := os.Stat(MainStaticBinary); err != nil {
		return false
	}
	if _, err := os.Stat(GolangLib); err != nil {
		return false
	}
	if _, err := os.Stat(ClientLib); err != nil {
		return false
	}
	return true
}
