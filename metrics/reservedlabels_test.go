package metrics

import (
	"os"
	"regexp"
	"testing"
)

// The site-aggregator stamps every federated sample with the site label set.
// A metric that carries one of these names itself makes the whole /federate
// payload invalid ("label name <x> is not unique: invalid sample"), which
// takes down the scrape for the ENTIRE site, not just the offending series —
// observed 2026-09-23 when bsp_beef_topics_total shipped a "role" dimension
// and both spine seams went down minutes after the converge.
var reservedSiteLabels = []string{"fabric", "geo", "location", "node", "region", "role", "site"}

func TestNoMetricUsesAReservedSiteLabel(t *testing.T) {
	src, err := os.ReadFile("metrics.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range reservedSiteLabels {
		re := regexp.MustCompile(`attribute\.(String|Int|Int64|Bool)\("` + name + `"`)
		if loc := re.FindIndex(src); loc != nil {
			line := 1
			for _, b := range src[:loc[0]] {
				if b == '\n' {
					line++
				}
			}
			t.Errorf("metrics.go:%d uses reserved site label %q as a metric dimension; "+
				"the site-aggregator adds it at federation, so this invalidates the whole site scrape. "+
				"Prefix it (e.g. %q)", line, name, "topic_"+name)
		}
	}
}
