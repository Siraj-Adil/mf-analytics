// Package mfapi provides a typed client for https://api.mfapi.in.
package mfapi

// SchemeListItem is an entry in the full scheme catalogue returned by GET /mf/.
type SchemeListItem struct {
	SchemeCode int    `json:"schemeCode"`
	SchemeName string `json:"schemeName"`
}

// SchemeDetail is the response from GET /mf/{code}.
type SchemeDetail struct {
	Meta   SchemeMeta  `json:"meta"`
	Data   []NAVEntry  `json:"data"`
	Status string      `json:"status"`
}

// SchemeMeta contains fund-level metadata.
type SchemeMeta struct {
	FundHouse       string `json:"fund_house"`
	SchemeType      string `json:"scheme_type"`
	SchemeCategory  string `json:"scheme_category"`
	SchemeCode      int    `json:"scheme_code"`
	SchemeName      string `json:"scheme_name"`
}

// NAVEntry is a single day's NAV from the API (date as DD-MM-YYYY string, nav as string).
type NAVEntry struct {
	Date string `json:"date"`
	NAV  string `json:"nav"`
}
