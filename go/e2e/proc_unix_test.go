//go:build e2e && !windows

package e2e

import (
	"os/exec"
	"syscall"
)

// prepareTree puts the process in its own process group so killTree reaches its children.
func prepareTree(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killTree sends SIGKILL to the whole process group.
func killTree(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
