package detector

import (
	"testing"
	"time"
)

func TestDetector_NewSourceNotAnomaly(t *testing.T) {
	d := New(10, 50*time.Millisecond, 2.0)
	defer d.Stop()

	// First ever event for a source: all buckets are zero → variance = 0 → z-score = 0.
	if d.Record("brand-new-source") {
		t.Error("first event for new source should not be anomalous (no baseline yet)")
	}
}

func TestDetector_SpikeAboveBaselineIsAnomaly(t *testing.T) {
	d := New(10, 50*time.Millisecond, 2.0)
	defer d.Stop()

	// Build a consistent baseline: ~10 events per bucket, 8 buckets.
	for i := 0; i < 8; i++ {
		for j := 0; j < 10; j++ {
			d.Record("payments")
		}
		time.Sleep(55 * time.Millisecond)
	}

	// Send a spike — far above the baseline mean.
	var gotAnomaly bool
	for i := 0; i < 100; i++ {
		if d.Record("payments") {
			gotAnomaly = true
			break
		}
	}
	if !gotAnomaly {
		t.Error("spike of 100 events should have triggered anomaly detection")
	}
}

func TestDetector_SourceIsolation(t *testing.T) {
	d := New(10, 50*time.Millisecond, 2.0)
	defer d.Stop()

	// Build baseline for source A.
	for i := 0; i < 8; i++ {
		for j := 0; j < 5; j++ {
			d.Record("sourceA")
		}
		time.Sleep(55 * time.Millisecond)
	}

	// Source B has no history — first event is not anomalous.
	if d.Record("sourceB") {
		t.Error("source B with no history should not be flagged as anomaly")
	}
}

func TestDetector_StopDoesNotPanic(t *testing.T) {
	d := New(5, 10*time.Millisecond, 2.0)
	d.Record("x")
	d.Stop()
}
