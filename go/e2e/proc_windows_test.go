//go:build e2e

package e2e

import (
	"os/exec"
	"strconv"
)

// prepareTree needs nothing on Windows; killTree removes the whole tree.
func prepareTree(*exec.Cmd) {}

// killTree stops the process and its children (the venv python.exe launcher starts a child).
func killTree(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
	_ = cmd.Process.Kill()
}
