package member

// Info is member info stored in etcd
type Info struct {
	Name       string   `json:"name"`        // PM server name
	MemberID   uint64   `json:"member_id"`   // Member.id
	PeerUrls   []string `json:"peer_urls"`   // Member.etcd.Config().APUrls
	ClientUrls []string `json:"client_urls"` // Member.etcd.Config().ACUrls
}
