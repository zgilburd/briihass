package admin

import (
	"fmt"
	"html/template"
	"time"
)

// templateFuncs is the function map available inside templates.
var templateFuncs = template.FuncMap{
	"fmtTime": func(t time.Time) string {
		if t.IsZero() {
			return "—"
		}
		return t.UTC().Format(time.RFC3339)
	},
	"fmtFloat": func(f float64) string {
		return fmt.Sprintf("%.2f", f)
	},
	"fmtAge": func(s float64) string {
		if s < 0 {
			return "—"
		}
		if s < 60 {
			return fmt.Sprintf("%.1fs", s)
		}
		return fmt.Sprintf("%.1fm", s/60)
	},
	// derefFloat / derefInt unwrap *T from override pointers so
	// templates can render them without choking on nil. Returns ""
	// for nil so the form input is empty (meaning "inherit default").
	"derefFloat": func(p *float64) string {
		if p == nil {
			return ""
		}
		return fmt.Sprintf("%v", *p)
	},
	"derefInt": func(p *int) string {
		if p == nil {
			return ""
		}
		return fmt.Sprintf("%d", *p)
	},
	"deref64": func(p *int64) int64 {
		if p == nil {
			return 0
		}
		return *p
	},
}
