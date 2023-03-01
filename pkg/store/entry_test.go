package store

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBaseEntry_MarshalJson(t *testing.T) {
	tests := []struct {
		name string
		args BaseEntry
		want []byte
	}{
		{
			name: "",
			args: BaseEntry{
				K:    "test",
				HIdx: 0,
			},
			want: []byte(`{"key":"test","heapIdx":0}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.args)
			if err != nil {
				t.Errorf("failed to marshal BaseEntry: %v", err)
			}
			assert.Equal(t, tt.want, b)
		})
	}
}

func TestBaseEntry_UnmarshalJson(t *testing.T) {
	tests := []struct {
		name string
		args []byte
		want BaseEntry
	}{
		{
			name: "",
			args: []byte(`{"key":"test","heapIdx":0}`),
			want: BaseEntry{
				K:    "test",
				HIdx: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got BaseEntry
			err := json.Unmarshal(tt.args, &got)
			if err != nil {
				t.Errorf("failed to unmarshal BaseEntry: %v", err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStringEntry_MarshalJson(t *testing.T) {
	tests := []struct {
		name string
		args StringEntry
		want []byte
	}{
		{
			name: "",
			args: StringEntry{
				BaseEntry: BaseEntry{
					K:    "test",
					HIdx: 0,
				},
				Val: "test",
			},
			want: []byte(`{"key":"test","heapIdx":0,"val":"test"}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.args)
			if err != nil {
				t.Errorf("failed to marshal BaseEntry: %v", err)
			}
			assert.Equal(t, tt.want, b)
		})
	}
}

func TestStringEntry_UnmarshalJson(t *testing.T) {
	tests := []struct {
		name string
		args []byte
		want StringEntry
	}{
		{
			name: "",
			args: []byte(`{"key":"test","heapIdx":0,"val":"test"}`),
			want: StringEntry{
				BaseEntry: BaseEntry{
					K:    "test",
					HIdx: 0,
				},
				Val: "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got StringEntry
			err := json.Unmarshal(tt.args, &got)
			if err != nil {
				t.Errorf("failed to unmarshal BaseEntry: %v", err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
