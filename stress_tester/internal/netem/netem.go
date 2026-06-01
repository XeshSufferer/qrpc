package netem

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/XeshSufferer/qrpc/stress_tester/internal/config"
)

var ErrNotPermitted = errors.New("operation not permitted")

type Emulator struct {
	iface string
}

func New(iface string) *Emulator {
	return &Emulator{iface: iface}
}

func (e *Emulator) Apply(profile config.NetworkProfile) error {
	if err := e.Clean(); err != nil {
		if isNotPermitted(err) {
			return ErrNotPermitted
		}
		return fmt.Errorf("clean before apply: %w", err)
	}

	args := []string{"qdisc", "add", "dev", e.iface, "root", "netem"}

	if profile.Delay != "" && profile.Delay != "0ms" {
		args = append(args, "delay", profile.Delay)
		if profile.Jitter != "" && profile.Jitter != "0ms" {
			args = append(args, profile.Jitter)
			if profile.Correlation > 0 {
				args = append(args, fmt.Sprintf("%.2f", profile.Correlation))
			}
		}
	}

	if profile.Loss > 0 {
		args = append(args, "loss", fmt.Sprintf("%.2f", profile.Loss))
	}

	if profile.Rate != "" {
		args = append(args, "rate", profile.Rate)
	}

	out, err := e.runOutput(args...)
	if err != nil {
		if strings.Contains(string(out), "RTNETLINK answers: File exists") {
			if err := e.Clean(); err != nil {
				return fmt.Errorf("clean after conflict: %w", err)
			}
			out, err = e.runOutput(args...)
			if err != nil {
				return fmt.Errorf("apply after retry: %w: %s", err, string(out))
			}
		} else {
			return fmt.Errorf("apply netem: %w: %s", err, string(out))
		}
	}

	return nil
}

func (e *Emulator) Clean() error {
	out, err := e.runOutput("qdisc", "del", "dev", e.iface, "root")
	if err != nil {
		if strings.Contains(string(out), "RTNETLINK answers: No such file or directory") ||
			strings.Contains(string(out), "Cannot delete qdisc with handle of zero") {
			return nil
		}
		if isNotPermitted(err) || isNotPermittedOutput(out) {
			return ErrNotPermitted
		}
		return fmt.Errorf("clean netem: %w: %s", err, string(out))
	}
	return nil
}

func (e *Emulator) Show() (string, error) {
	out, err := e.runOutput("qdisc", "show", "dev", e.iface)
	if err != nil {
		return "", fmt.Errorf("show qdisc: %w", err)
	}
	return string(out), nil
}

func (e *Emulator) run(args ...string) error {
	cmd := exec.Command("tc", args...)
	return cmd.Run()
}

func (e *Emulator) runOutput(args ...string) ([]byte, error) {
	cmd := exec.Command("tc", args...)
	return cmd.CombinedOutput()
}

func isNotPermitted(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Operation not permitted") ||
		strings.Contains(err.Error(), "permission denied")
}

func isNotPermittedOutput(out []byte) bool {
	return strings.Contains(string(out), "Operation not permitted")
}
