package reposerverextract

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCachingPuller(t *testing.T, inner chartPuller) *cachingChartPuller {
	t.Helper()
	puller, cleanup, err := newCachingChartPuller(inner)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return puller
}

func chartSource(chart, version string) v1alpha1.ApplicationSource {
	return v1alpha1.ApplicationSource{RepoURL: "123.dkr.ecr.eu-west-1.amazonaws.com", Chart: chart, TargetRevision: version}
}

func TestCachingChartPuller_PullsSameChartOnce(t *testing.T) {
	inner := &fakeChartPuller{files: map[string]string{
		"Chart.yaml":             "apiVersion: v2\nname: svc\nversion: 1.0.0\n",
		"templates/service.yaml": "kind: Service\n",
	}}
	puller := newTestCachingPuller(t, inner)

	for range 3 {
		destDir := t.TempDir()
		chartDir, err := puller.Pull(chartSource("svc", "1.0.0"), nil, destDir)
		require.NoError(t, err)

		// Same layout under destDir as the inner puller produces.
		assert.Equal(t, filepath.Join(destDir, "chart", "svc"), chartDir)
		content, err := os.ReadFile(filepath.Join(chartDir, "templates", "service.yaml"))
		require.NoError(t, err)
		assert.Equal(t, "kind: Service\n", string(content))
	}

	assert.Len(t, inner.pulled, 1)
}

func TestCachingChartPuller_DifferentVersionsPulledSeparately(t *testing.T) {
	inner := &fakeChartPuller{}
	puller := newTestCachingPuller(t, inner)

	for _, source := range []v1alpha1.ApplicationSource{
		chartSource("svc", "1.0.0"),
		chartSource("svc", "1.1.0"),
		chartSource("other", "1.0.0"),
		chartSource("svc", "1.0.0"),
	} {
		_, err := puller.Pull(source, nil, t.TempDir())
		require.NoError(t, err)
	}

	assert.Len(t, inner.pulled, 3)
}

// flakyChartPuller fails its first `failures` pulls, then delegates to a
// fakeChartPuller. It is safe for concurrent use.
type flakyChartPuller struct {
	mu       sync.Mutex
	failures int
	calls    atomic.Int32
	inner    fakeChartPuller
}

func (f *flakyChartPuller) Pull(source v1alpha1.ApplicationSource, creds *RepoCreds, destDir string) (string, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		// Leave a partial file behind, as an interrupted pull might.
		_ = os.WriteFile(filepath.Join(destDir, "partial"), []byte("x"), 0o644)
		return "", errors.New("registry unavailable")
	}
	return f.inner.Pull(source, creds, destDir)
}

func TestCachingChartPuller_FailedPullIsNotCached(t *testing.T) {
	inner := &flakyChartPuller{failures: 1}
	puller := newTestCachingPuller(t, inner)

	_, err := puller.Pull(chartSource("svc", "1.0.0"), nil, t.TempDir())
	require.Error(t, err)

	destDir := t.TempDir()
	chartDir, err := puller.Pull(chartSource("svc", "1.0.0"), nil, destDir)
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(chartDir, "Chart.yaml"))
	assert.NoFileExists(t, filepath.Join(destDir, "partial"))

	assert.Equal(t, int32(2), inner.calls.Load())
}

func TestCachingChartPuller_ConcurrentCallersShareOnePull(t *testing.T) {
	inner := &flakyChartPuller{}
	puller := newTestCachingPuller(t, inner)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			chartDir, err := puller.Pull(chartSource("svc", "1.0.0"), nil, t.TempDir())
			assert.NoError(t, err)
			assert.FileExists(t, filepath.Join(chartDir, "Chart.yaml"))
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), inner.calls.Load())
}
