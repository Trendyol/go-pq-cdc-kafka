package packaging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type workflowStep struct {
	Name string            `yaml:"name"`
	Uses string            `yaml:"uses"`
	Run  string            `yaml:"run"`
	With map[string]string `yaml:"with"`
}
type workflowJob struct {
	If          string            `yaml:"if"`
	Uses        string            `yaml:"uses"`
	Needs       string            `yaml:"needs"`
	Permissions map[string]string `yaml:"permissions"`
	Secrets     string            `yaml:"secrets"`
	Steps       []workflowStep    `yaml:"steps"`
}
type buildWorkflow struct {
	Name string                 `yaml:"name"`
	On   map[string]any         `yaml:"on"`
	Jobs map[string]workflowJob `yaml:"jobs"`
}

func TestOnlineStagingBuildWorkflowPreservesDefaultAndIsolatesApprovedDispatch(t *testing.T) {
	file := filepath.Join("..", "..", ".github", "workflows", "build.yml")
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	var workflow buildWorkflow
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	require.Equal(t, "Build", workflow.Name)
	require.Contains(t, workflow.On, "workflow_dispatch")
	require.Nil(t, workflow.On["workflow_dispatch"], "manual dispatch adds no registration-dependent inputs")
	require.Equal(t, []any{"main"}, workflow.On["push"].(map[string]any)["branches"])
	require.Equal(t, []any{"main"}, workflow.On["pull_request"].(map[string]any)["branches"])
	require.Equal(t, []any{"opened", "reopened", "synchronize"}, workflow.On["pull_request"].(map[string]any)["types"])
	normalized := func(value string) string { return strings.Join(strings.Fields(value), " ") }
	route := "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/build/online-staging-kafka-20261113'"
	require.Equal(t, "${{ "+route+" }}", normalized(workflow.Jobs["online-staging-archive"].If))
	require.Equal(t, "${{ !("+route+") }}", normalized(workflow.Jobs["build"].If))
	build := workflow.Jobs["build"]
	require.Equal(t, "actions/checkout@v7", build.Steps[0].Uses)
	require.Equal(t, "1.25.0", build.Steps[1].With["go-version"])
	require.Contains(t, build.Steps[2].Run, "make lint")
	require.Contains(t, build.Steps[3].Run, "make tidy")
	require.Contains(t, build.Steps[4].Run, "make build/linux")
	security := workflow.Jobs["security-gates"]
	require.Equal(t, "Trendyol/security-actions/.github/workflows/security-gates.yml@master", security.Uses)
	require.Equal(t, "build", security.Needs)
	require.Equal(t, "inherit", security.Secrets, "inherited contract unchanged and naturally skipped only on isolated staging route")
}

func TestOnlineStagingWorkflowPinsActionsSourcesAndHasNoPublicationPrivileges(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "build.yml"))
	require.NoError(t, err)
	var workflow buildWorkflow
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	stage := workflow.Jobs["online-staging-archive"]
	require.Equal(t, map[string]string{"contents": "read"}, stage.Permissions)
	require.Empty(t, stage.Secrets)
	allowed := map[string]map[string]bool{
		"actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1":           {"ref": true, "path": true, "persist-credentials": true, "repository": true},
		"actions/setup-go@d8cbb8045d7f43706b9177cceb36c1f91a6644d5":           {"go-version": true, "cache": true},
		"docker/setup-buildx-action@f87e5991a6d7451dcb8d9637bfbc97413f497069": {"version": true, "driver": true, "driver-opts": true, "buildkitd-flags": true, "cache-binary": true},
		"actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9":    {"name": true, "path": true, "if-no-files-found": true, "retention-days": true, "compression-level": true},
	}
	checkouts := 0
	var scripts strings.Builder
	for _, step := range stage.Steps {
		scripts.WriteString(step.Run)
		if step.Uses == "" {
			continue
		}
		inputs, ok := allowed[step.Uses]
		require.True(t, ok, "staging actions must use the actually verified immutable refs")
		for key := range step.With {
			require.True(t, inputs[key], "input %s not in verified action metadata", key)
		}
		switch {
		case strings.HasPrefix(step.Uses, "actions/checkout@"):
			checkouts++
			require.Equal(t, "false", step.With["persist-credentials"])
			if step.With["path"] == "kafka" {
				require.Equal(t, "${{ github.sha }}", step.With["ref"])
			} else {
				require.Equal(t, "${{ steps.binding.outputs.core_sha }}", step.With["ref"])
			}
		case strings.HasPrefix(step.Uses, "actions/setup-go@"):
			require.Equal(t, "1.26.10", step.With["go-version"])
			require.Equal(t, "false", step.With["cache"])
		case strings.HasPrefix(step.Uses, "docker/setup-buildx-action@"):
			require.Equal(t, "docker-container", step.With["driver"])
			require.Equal(t, "v0.39.0", step.With["version"])
			require.Equal(t, "false", step.With["cache-binary"])
			require.NotContains(t, step.With["buildkitd-flags"], "allow-insecure-entitlement")
		case strings.HasPrefix(step.Uses, "actions/upload-artifact@"):
			require.Contains(t, step.With["name"], "${{ github.sha }}")
			require.Equal(t, "error", step.With["if-no-files-found"])
			require.Equal(t, "7", step.With["retention-days"])
		}
	}
	require.Equal(t, 2, checkouts)
	script := scripts.String()
	for _, unsafe := range []string{"secrets.", "docker login", "--push", "git push", "gh workflow", "make tidy"} {
		require.NotContains(t, script, unsafe)
	}
	require.Contains(t, script, "source_binding.py")
	require.Contains(t, script, "--integration")
	require.Contains(t, script, "POSTGRES_VERSION=17-alpine", "core fixture selector must not be confused with Kafka selector")
	require.Contains(t, script, "POSTGRES_TEST_IMAGE=16-alpine")
	require.Contains(t, script, "verify_tools.py")
	require.Contains(t, script, "--verify-buildx")
	require.Contains(t, script, "sha256sum -c SHA256SUMS")
}
