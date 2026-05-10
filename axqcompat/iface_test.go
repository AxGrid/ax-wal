package axqcompat

import (
	"github.com/axgrid/axq"
)

// Compile-time assertions: if either of these declarations stops type-
// checking, our adapter has drifted out of sync with github.com/axgrid/axq's
// Writer / Reader interfaces and the next test run will fail to build.
var (
	_ axq.Writer = (*Writer)(nil)
	_ axq.Reader = (*Reader)(nil)
)
