package resp2

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func getAllReplies(p *Parser) (res []Reply) {
	for reply, err := p.Parse(); err == nil; reply, err = p.Parse() {
		res = append(res, reply)
	}
	return
}

func equalReply(t *testing.T, expected, actual Reply) {
	assert.Equal(t, expected.Type(), actual.Type(), "different type")
	switch expected.(type) {
	case *SimpleStringReply:
		er := expected.(*SimpleStringReply)
		ar := actual.(*SimpleStringReply)
		assert.Equal(t, er.Value, ar.Value)
	case *ErrorReply:
		er := expected.(*ErrorReply)
		ar := actual.(*ErrorReply)
		assert.Equal(t, er.Value, ar.Value)
	case *IntegerReply:
		er := expected.(*IntegerReply)
		ar := actual.(*IntegerReply)
		assert.Equal(t, er.Value, ar.Value)
	case *BulkStringReply:
		er := expected.(*BulkStringReply)
		ar := actual.(*BulkStringReply)
		assert.Equal(t, er.Value, ar.Value)
	case *ArrayReply:
		er := expected.(*ArrayReply)
		ar := actual.(*ArrayReply)
		assert.Equal(t, er.Len, ar.Len)
		for i := 0; i < er.Len; i++ {
			equalReply(t, er.Value[i], ar.Value[i])
		}
	}
}

func TestParser_parseSimpleString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Reply
	}{
		{
			name:  "일반 문자열",
			input: "+OK\r\n+Hello world\r\n",
			want: []Reply{
				&SimpleStringReply{"OK"},
				&SimpleStringReply{"Hello world"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			got := getAllReplies(p)

			assert.Equal(t, len(tt.want), len(got))
			for i := 0; i < len(got); i++ {
				equalReply(t, tt.want[i], got[i])
			}
		})
	}
}

func TestParser_parseError(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Reply
	}{
		{
			name:  "일반 에러",
			input: "-Error message\r\n",
			want: []Reply{
				&ErrorReply{"Error message"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			got := getAllReplies(p)

			assert.Equal(t, len(tt.want), len(got))
			for i := 0; i < len(got); i++ {
				equalReply(t, tt.want[i], got[i])
			}
		})
	}
}

func TestParser_parseInteger(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Reply
	}{
		{
			name:  "일반 정수",
			input: ":0\r\n:1000\r\n",
			want: []Reply{
				&IntegerReply{0},
				&IntegerReply{1000},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			got := getAllReplies(p)

			assert.Equal(t, len(tt.want), len(got))
			for i := 0; i < len(got); i++ {
				equalReply(t, tt.want[i], got[i])
			}
		})
	}
}

func TestParser_parseBulkString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Reply
	}{
		{
			name:  "일반 문자열",
			input: "$5\r\nhello\r\n$5\nworld\n",
			want: []Reply{
				&BulkStringReply{5, "hello"},
				&BulkStringReply{5, "world"},
			},
		},
		{
			name:  "빈 문자열",
			input: "$0\r\n\r\n$0\r\n\r\n",
			want: []Reply{
				&BulkStringReply{0, ""},
				&BulkStringReply{0, ""},
			},
		},
		{
			name:  "널 문자열",
			input: "$-1\r\n$-1\n",
			want: []Reply{
				&BulkStringReply{Len: -1},
				&BulkStringReply{Len: -1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			got := getAllReplies(p)

			assert.Equal(t, len(tt.want), len(got))
			for i := 0; i < len(got); i++ {
				equalReply(t, tt.want[i], got[i])
			}
		})
	}
}

func TestParser_ParseArray(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Reply
	}{
		{
			name:  "일반 배열",
			input: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n*3\r\n:1\r\n:2\r\n:3\r\n",
			want: []Reply{
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
		},
		{
			name:  "빈 배열",
			input: "*0\r\n*0\r\n",
			want: []Reply{
				&ArrayReply{
					Len:   0,
					Value: []Reply{},
				},
				&ArrayReply{
					Len:   0,
					Value: []Reply{},
				},
			},
		},
		{
			name:  "널 배열",
			input: "*-1\r\n*-1\r\n",
			want: []Reply{
				&ArrayReply{Len: -1},
				&ArrayReply{Len: -1},
			},
		},
		{
			name:  "중첩 배열",
			input: "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
			want: []Reply{
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
		},
		{
			name:  "널 요소 배열",
			input: "*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n",
			want: []Reply{
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
			p := NewParser(strings.NewReader(tt.input))
			got := getAllReplies(p)

			assert.Equal(t, len(tt.want), len(got))
			for i := 0; i < len(got); i++ {
				equalReply(t, tt.want[i], got[i])
			}
		})
	}
}
