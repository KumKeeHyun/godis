package command

import (
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseSet(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "set k v",
			input: []resp.Reply{
				resp.NewBulkStringReply("set"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v"),
			},
			want: &Set{
				Key: "k",
				Val: "v",
			},
		},
		{
			name: "set k v nx get",
			input: []resp.Reply{
				resp.NewBulkStringReply("set"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v"),
				resp.NewBulkStringReply("nx"),
				resp.NewBulkStringReply("get"),
			},
			want: &Set{
				Key: "k",
				Val: "v",
				Mod: "nx",
				Get: true,
			},
		},
		{
			name: "set k v ex 100",
			input: []resp.Reply{
				resp.NewBulkStringReply("set"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v"),
				resp.NewBulkStringReply("ex"),
				resp.NewBulkStringReply("100"),
			},
			want: &Set{
				Key: "k",
				Val: "v",
				TTL: 100 * time.Second,
			},
		},
		{
			name: "set k v exat 1000000",
			input: []resp.Reply{
				resp.NewBulkStringReply("set"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v"),
				resp.NewBulkStringReply("exat"),
				resp.NewBulkStringReply("1000000"),
			},
			want: &Set{
				Key:      "k",
				Val:      "v",
				ExpireAt: time.Unix(1000000, 0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isSet := parseSet(tt.input).(*Set)
			want, wIsSet := tt.want.(*Set)

			if !isSet && !wIsSet {
				return
			}

			assert.Equal(t, want.Key, got.Key)
			assert.Equal(t, want.Val, got.Val)
			assert.Equal(t, want.Mod, got.Mod)
			assert.Equal(t, want.Get, got.Get)
			assert.Equal(t, want.TTL, got.TTL)
			assert.Equal(t, want.ExpireAt, got.ExpireAt)
			assert.Equal(t, want.KeepTTL, got.KeepTTL)
		})
	}
}

func TestSet_expire(t *testing.T) {
	tests := []struct {
		name     string
		input    *Set
		want     time.Time
		isExpire bool
	}{
		{
			name:     "no expire",
			input:    &Set{},
			want:     time.Time{},
			isExpire: false,
		},
		{
			name: "ttl",
			input: &Set{
				From:    time.Unix(0, 0),
				TTL:     100 * time.Second,
				KeepTTL: false,
			},
			want:     time.Unix(0, 0).Add(100 * time.Second),
			isExpire: true,
		},
		{
			name: "expire at",
			input: &Set{
				ExpireAt: time.Unix(10, 0),
				KeepTTL:  false,
			},
			want:     time.Unix(10, 0),
			isExpire: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isExpire := tt.input.expire()
			assert.Equalf(t, tt.isExpire, isExpire, "isExpire want: %v, got: %v", tt.isExpire, isExpire)
			if isExpire {
				assert.Equalf(t, tt.want, got, "expireTime want: %v, got: %v", tt.want, got)
			}
		})
	}
}

func TestParseGet(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "get k",
			input: []resp.Reply{
				resp.NewBulkStringReply("get"),
				resp.NewBulkStringReply("k"),
			},
			want: &Get{
				Key: "k",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isGet := parseGet(tt.input).(*Get)
			want, wIsGet := tt.want.(*Get)

			if !isGet && !wIsGet {
				return
			}
			assert.Equal(t, want.Key, got.Key)
		})
	}
}

func TestParseMGet(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "mget k1 k2 k3",
			input: []resp.Reply{
				resp.NewBulkStringReply("mget"),
				resp.NewBulkStringReply("k1"),
				resp.NewBulkStringReply("k2"),
				resp.NewBulkStringReply("k3"),
			},
			want: &MGet{
				Keys: []string{"k1", "k2", "k3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isMGet := parseMGet(tt.input).(*MGet)
			want, wIsMGet := tt.want.(*MGet)

			if !isMGet && !wIsMGet {
				return
			}
			assert.ElementsMatch(t, want.Keys, got.Keys)
		})
	}
}
