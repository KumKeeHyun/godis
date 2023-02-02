package resp2

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func writeAllReplies(t *testing.T, w *ReplyWriter, rs []Reply) {
	for _, r := range rs {
		if err := w.Write(r); err != nil {
			t.Error(err)
		}
	}
}

func TestReplyWriter_Write_simpleString(t *testing.T) {
	tests := []struct {
		name  string
		input []Reply
		want  string
	}{
		{
			name: "일반 문자열",
			input: []Reply{
				&SimpleStringReply{"OK"},
				&SimpleStringReply{"Hello world"},
			},
			want: "+OK\r\n+Hello world\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			rw := NewReplyWriter(buf)

			writeAllReplies(t, rw, tt.input)

			assert.Equal(t, tt.want, buf.String())
		})
	}
}

func TestReplyWriter_Write_error(t *testing.T) {
	tests := []struct {
		name  string
		input []Reply
		want  string
	}{
		{
			name: "일반 에러",
			input: []Reply{
				&ErrorReply{"Error message"},
			},
			want: "-Error message\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			rw := NewReplyWriter(buf)

			writeAllReplies(t, rw, tt.input)

			assert.Equal(t, tt.want, buf.String())
		})
	}
}

func TestReplyWriter_Write_integer(t *testing.T) {
	tests := []struct {
		name  string
		input []Reply
		want  string
	}{
		{
			name: "일반 정수",
			input: []Reply{
				&IntegerReply{0},
				&IntegerReply{1000},
			},
			want: ":0\r\n:1000\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			rw := NewReplyWriter(buf)

			writeAllReplies(t, rw, tt.input)

			assert.Equal(t, tt.want, buf.String())
		})
	}
}

func TestReplyWriter_Write_bulkString(t *testing.T) {
	tests := []struct {
		name  string
		input []Reply
		want  string
	}{
		{
			name: "일반 문자열",
			input: []Reply{
				&BulkStringReply{5, "hello"},
				&BulkStringReply{5, "world"},
			},
			want: "$5\r\nhello\r\n$5\r\nworld\r\n",
		},
		{
			name: "빈 문자열",
			input: []Reply{
				&BulkStringReply{0, ""},
				&BulkStringReply{0, ""},
			},
			want: "$0\r\n\r\n$0\r\n\r\n",
		},
		{
			name: "널 문자열",
			input: []Reply{
				&BulkStringReply{Len: -1},
				&BulkStringReply{Len: -1},
			},
			want: "$-1\r\n$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			rw := NewReplyWriter(buf)

			writeAllReplies(t, rw, tt.input)

			assert.Equal(t, tt.want, buf.String())
		})
	}
}

func TestReplyWriter_Write_array(t *testing.T) {
	tests := []struct {
		name  string
		input []Reply
		want  string
	}{
		{
			name: "일반 배열",
			input: []Reply{
				&ArrayReply{
					Len: 2,
					Value: []Reply{
						&BulkStringReply{5, "hello"},
						&BulkStringReply{5, "world"},
					},
				},
				&ArrayReply{
					Len: 3,
					Value: []Reply{
						&IntegerReply{1},
						&IntegerReply{2},
						&IntegerReply{3},
					},
				},
			},
			want: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n*3\r\n:1\r\n:2\r\n:3\r\n",
		},
		{
			name: "빈 배열",
			input: []Reply{
				&ArrayReply{
					Len:   0,
					Value: []Reply{},
				},
				&ArrayReply{
					Len:   0,
					Value: []Reply{},
				},
			},
			want: "*0\r\n*0\r\n",
		},
		{
			name: "널 배열",
			input: []Reply{
				&ArrayReply{Len: -1},
				&ArrayReply{Len: -1},
			},
			want: "*-1\r\n*-1\r\n",
		},
		{
			name: "중첩 배열",
			input: []Reply{
				&ArrayReply{
					Len: 2,
					Value: []Reply{
						&ArrayReply{
							Len: 3,
							Value: []Reply{
								&IntegerReply{1},
								&IntegerReply{2},
								&IntegerReply{3},
							},
						},
						&ArrayReply{
							Len: 2,
							Value: []Reply{
								&SimpleStringReply{"Hello"},
								&ErrorReply{"World"},
							},
						},
					},
				},
			},
			want: "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
		},
		{
			name: "널 요소 배열",
			want: "*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n",
			input: []Reply{
				&ArrayReply{
					Len: 3,
					Value: []Reply{
						&BulkStringReply{5, "hello"},
						&BulkStringReply{Len: -1},
						&BulkStringReply{5, "world"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			rw := NewReplyWriter(buf)

			writeAllReplies(t, rw, tt.input)

			assert.Equal(t, tt.want, buf.String())
		})
	}
}
