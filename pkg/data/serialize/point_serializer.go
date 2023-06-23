package serialize

import (
	"io"

	"github.com/timescale/tsbs/pkg/data"
)

// PointSerializer serializes a Point for writing
type PointSerializer interface {
	Serialize(p *data.Point, w io.Writer) error
}
