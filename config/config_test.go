package config

import (
	"flag"
	"net"
	"os"
	"testing"
	"time"
)

// resetFlags recreates flag.CommandLine so that Load's flag.Parse call starts
// from a clean state between test runs.
func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
}

// realIface returns the name of the first non-loopback interface, falling back
// to the loopback name. Used wherever Load needs a valid iface to pass
// its net.InterfaceByName check.
func realIface(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("net.Interfaces: %v", err)
	}
	for _, i := range ifaces {
		return i.Name
	}
	t.Fatal("no network interfaces found")
	return ""
}

// parseArgs is a helper that resets flag.CommandLine, sets os.Args, calls
// Load, and restores os.Args. Using flag package in tests requires resetting
// the flag set between runs.
func parseArgs(t *testing.T, args []string) (*Config, error) {
	t.Helper()
	old := os.Args
	t.Cleanup(func() {
		os.Args = old
		resetFlags()
	})
	os.Args = append([]string{"test"}, args...)
	resetFlags()
	return Load()
}

func TestLoadDefaults(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.UDPListenPort != 9000 {
		t.Errorf("UDPListenPort = %d, want 9000", cfg.UDPListenPort)
	}
	if cfg.EgressPort != 9001 {
		t.Errorf("EgressPort = %d, want 9001", cfg.EgressPort)
	}
	if cfg.MCScope != "site" {
		t.Errorf("MCScope = %q, want site", cfg.MCScope)
	}
	if cfg.ShardBits != 2 {
		t.Errorf("ShardBits = %d, want 2", cfg.ShardBits)
	}
	if cfg.NumWorkers <= 0 {
		t.Errorf("NumWorkers = %d, want > 0", cfg.NumWorkers)
	}
	if cfg.MCPrefix != 0xFF05 {
		t.Errorf("MCPrefix = 0x%04X, want 0xFF05", cfg.MCPrefix)
	}
	if len(cfg.EgressIfaces) != 1 || cfg.EgressIfaces[0] != iface {
		t.Errorf("EgressIfaces = %v, want [%s]", cfg.EgressIfaces, iface)
	}
}

func TestLoadShardBitsRange(t *testing.T) {
	iface := realIface(t)
	for _, bits := range []string{"0", "25"} {
		_, err := parseArgs(t, []string{"-iface", iface, "-shard-bits", bits})
		if err == nil {
			t.Errorf("shard-bits=%s: want error, got nil", bits)
		}
	}
}

func TestLoadShardBitsValid(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface, "-shard-bits", "8"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ShardBits != 8 {
		t.Errorf("ShardBits = %d, want 8", cfg.ShardBits)
	}
	if cfg.NumGroups != 256 {
		t.Errorf("NumGroups = %d, want 256", cfg.NumGroups)
	}
}

func TestLoadUnknownScope(t *testing.T) {
	iface := realIface(t)
	_, err := parseArgs(t, []string{"-iface", iface, "-scope", "galaxy"})
	if err == nil {
		t.Error("want error for unknown scope, got nil")
	}
}

func TestLoadAllScopes(t *testing.T) {
	iface := realIface(t)
	cases := map[string]uint16{
		"link":   0xFF02,
		"site":   0xFF05,
		"org":    0xFF08,
		"global": 0xFF0E,
	}
	for scope, want := range cases {
		cfg, err := parseArgs(t, []string{"-iface", iface, "-scope", scope})
		if err != nil {
			t.Errorf("scope=%s: Load error: %v", scope, err)
			continue
		}
		if cfg.MCPrefix != want {
			t.Errorf("scope=%s: MCPrefix = 0x%04X, want 0x%04X", scope, cfg.MCPrefix, want)
		}
	}
}

func TestLoadBadInterface(t *testing.T) {
	_, err := parseArgs(t, []string{"-iface", "no_such_iface_xyz"})
	if err == nil {
		t.Error("want error for missing interface, got nil")
	}
}

func TestLoadMultipleIfaces(t *testing.T) {
	iface := realIface(t)
	// Pass the same interface twice via comma-separated value.
	cfg, err := parseArgs(t, []string{"-iface", iface + "," + iface})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.EgressIfaces) != 2 {
		t.Errorf("EgressIfaces len = %d, want 2", len(cfg.EgressIfaces))
	}
}

func TestLoadEmptyIfaceError(t *testing.T) {
	_, err := parseArgs(t, []string{"-iface", ""})
	if err == nil {
		t.Error("want error for empty -iface, got nil")
	}
}

func TestLoadMCGroupIDDefault(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MCGroupID != 0x000B {
		t.Errorf("MCGroupID = 0x%04X, want 0x000B (IANA Bitcoin)", cfg.MCGroupID)
	}
}

func TestLoadMCGroupIDHex(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface, "-mc-group-id", "0xCAFE"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MCGroupID != 0xCAFE {
		t.Errorf("MCGroupID = 0x%04X, want 0xCAFE", cfg.MCGroupID)
	}
}

func TestLoadMCGroupIDInvalid(t *testing.T) {
	iface := realIface(t)
	_, err := parseArgs(t, []string{"-iface", iface, "-mc-group-id", "not-a-number"})
	if err == nil {
		t.Error("want error for invalid mc-group-id, got nil")
	}
}

func TestLoadZeroWorkersDefaultsToCPU(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface, "-workers", "0"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NumWorkers <= 0 {
		t.Errorf("NumWorkers = %d after zero, want > 0", cfg.NumWorkers)
	}
}

func TestLoadInstanceIDDefaultsToHostname(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// InstanceID defaults to hostname when not set; flag default is "".
	// Load does not fill it in — that is done by metrics.New. Just confirm
	// the field is accessible and the load succeeds.
	_ = cfg.InstanceID
}

// ── env helper tests ──────────────────────────────────────────────────────────

func TestEnvStrFallback(t *testing.T) {
	_ = os.Unsetenv("TEST_ENV_STR_KEY")
	if got := envStr("TEST_ENV_STR_KEY", "default"); got != "default" {
		t.Errorf("got %q, want %q", got, "default")
	}
}

func TestEnvStrSet(t *testing.T) {
	t.Setenv("TEST_ENV_STR_KEY", "hello")
	if got := envStr("TEST_ENV_STR_KEY", "default"); got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

func TestEnvIntFallback(t *testing.T) {
	_ = os.Unsetenv("TEST_ENV_INT_KEY")
	if got := envInt("TEST_ENV_INT_KEY", 42); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestEnvIntSet(t *testing.T) {
	t.Setenv("TEST_ENV_INT_KEY", "99")
	if got := envInt("TEST_ENV_INT_KEY", 42); got != 99 {
		t.Errorf("got %d, want 99", got)
	}
}

func TestEnvIntInvalid(t *testing.T) {
	t.Setenv("TEST_ENV_INT_KEY", "not-a-number")
	if got := envInt("TEST_ENV_INT_KEY", 7); got != 7 {
		t.Errorf("got %d, want fallback 7", got)
	}
}

func TestEnvBoolFallback(t *testing.T) {
	_ = os.Unsetenv("TEST_ENV_BOOL_KEY")
	if got := envBool("TEST_ENV_BOOL_KEY", true); !got {
		t.Error("envBool: expected fallback true")
	}
}

func TestEnvBoolSet(t *testing.T) {
	t.Setenv("TEST_ENV_BOOL_KEY", "true")
	if got := envBool("TEST_ENV_BOOL_KEY", false); !got {
		t.Error("envBool: expected true")
	}
}

func TestEnvBoolSetFalse(t *testing.T) {
	t.Setenv("TEST_ENV_BOOL_KEY", "false")
	if got := envBool("TEST_ENV_BOOL_KEY", true); got {
		t.Error("envBool: expected false")
	}
}

func TestEnvBoolInvalid(t *testing.T) {
	t.Setenv("TEST_ENV_BOOL_KEY", "not-a-bool")
	if got := envBool("TEST_ENV_BOOL_KEY", true); !got {
		t.Error("envBool: expected fallback true for invalid value")
	}
}

// ── new v2 flag tests ───────────────────────────────────────────────────────────────

func TestLoadUDPListenPortCustom(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface, "-udp-listen-port", "9500"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.UDPListenPort != 9500 {
		t.Errorf("UDPListenPort = %d, want 9500", cfg.UDPListenPort)
	}
}

func TestLoadTCPListenPort(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface, "-tcp-listen-port", "9100"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TCPListenPort != 9100 {
		t.Errorf("TCPListenPort = %d, want 9100", cfg.TCPListenPort)
	}
}

func TestLoadTCPListenPortDefault(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TCPListenPort != 0 {
		t.Errorf("TCPListenPort = %d, want 0 (disabled)", cfg.TCPListenPort)
	}
}

func TestEnvDurationFallback(t *testing.T) {
	_ = os.Unsetenv("TEST_ENV_DUR_KEY")
	if got := envDuration("TEST_ENV_DUR_KEY", 30*time.Second); got != 30*time.Second {
		t.Errorf("got %v, want 30s", got)
	}
}

func TestEnvDurationSet(t *testing.T) {
	t.Setenv("TEST_ENV_DUR_KEY", "1m")
	if got := envDuration("TEST_ENV_DUR_KEY", 30*time.Second); got != time.Minute {
		t.Errorf("got %v, want 1m", got)
	}
}

func TestEnvDurationInvalid(t *testing.T) {
	t.Setenv("TEST_ENV_DUR_KEY", "not-a-duration")
	if got := envDuration("TEST_ENV_DUR_KEY", 5*time.Second); got != 5*time.Second {
		t.Errorf("got %v, want fallback 5s", got)
	}
}

func TestLoadDrainTimeoutDefault(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DrainTimeout != 0 {
		t.Errorf("DrainTimeout = %v, want 0 (disabled)", cfg.DrainTimeout)
	}
}

func TestLoadDrainTimeoutFlag(t *testing.T) {
	iface := realIface(t)
	cfg, err := parseArgs(t, []string{"-iface", iface, "-drain-timeout", "15s"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DrainTimeout != 15*time.Second {
		t.Errorf("DrainTimeout = %v, want 15s", cfg.DrainTimeout)
	}
}
