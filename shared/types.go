package shared

type OBUData struct {
	OBUId int     `json:"obu_id"`
	Lat   float64 `json:"lat"`
	Lng   float64 `json:"lng"`
}
