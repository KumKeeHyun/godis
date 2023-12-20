package command

import (
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSAdd(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "sadd k v",
			input: []resp.Reply{
				resp.NewBulkStringReply("sadd"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v"),
			},
			want: &SAdd{
				Key:     "k",
				Members: []string{"v"},
			},
		},
		{
			name: "sadd k v1 v2 v3",
			input: []resp.Reply{
				resp.NewBulkStringReply("sadd"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v1"),
				resp.NewBulkStringReply("v2"),
				resp.NewBulkStringReply("v3"),
			},
			want: &SAdd{
				Key:     "k",
				Members: []string{"v1", "v2", "v3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isSAdd := parseSAdd(tt.input).(*SAdd)
			want, wIsSAdd := tt.want.(*SAdd)

			if !isSAdd && !wIsSAdd {
				return
			}

			assert.Equal(t, want.Key, got.Key)
			assert.ElementsMatch(t, want.Members, got.Members)
		})
	}
}

func TestParseSCard(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "scard k",
			input: []resp.Reply{
				resp.NewBulkStringReply("scard"),
				resp.NewBulkStringReply("k"),
			},
			want: &SCard{
				Key: "k",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isSCard := parseSCard(tt.input).(*SCard)
			want, wIsSCard := tt.want.(*SCard)

			if !isSCard && !wIsSCard {
				return
			}

			assert.Equal(t, want.Key, got.Key)
		})
	}
}

func TestParseSMembers(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "smembers k",
			input: []resp.Reply{
				resp.NewBulkStringReply("smembers"),
				resp.NewBulkStringReply("k"),
			},
			want: &SMembers{
				Key: "k",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isSMembers := parseSMembers(tt.input).(*SMembers)
			want, wIsSMembers := tt.want.(*SMembers)

			if !isSMembers && !wIsSMembers {
				return
			}

			assert.Equal(t, want.Key, got.Key)
		})
	}
}

func TestParseSRem(t *testing.T) {
	tests := []struct {
		name  string
		input []resp.Reply
		want  Command
	}{
		{
			name: "srem k v",
			input: []resp.Reply{
				resp.NewBulkStringReply("srem"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v"),
			},
			want: &SRem{
				Key:     "k",
				Members: []string{"v"},
			},
		},
		{
			name: "srem k v1 v2 v3",
			input: []resp.Reply{
				resp.NewBulkStringReply("srem"),
				resp.NewBulkStringReply("k"),
				resp.NewBulkStringReply("v1"),
				resp.NewBulkStringReply("v2"),
				resp.NewBulkStringReply("v3"),
			},
			want: &SRem{
				Key:     "k",
				Members: []string{"v1", "v2", "v3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isSRem := parseSRem(tt.input).(*SRem)
			want, wIsSRem := tt.want.(*SRem)

			if !isSRem && !wIsSRem {
				return
			}

			assert.Equal(t, want.Key, got.Key)
			assert.ElementsMatch(t, want.Members, got.Members)
		})
	}
}
