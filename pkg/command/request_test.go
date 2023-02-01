package command

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequest(t *testing.T) {
	cmd := &Request{
		data: []string{"hello", "redis"},
	}

	buf := new(bytes.Buffer)
	bw := bufio.NewWriter(buf)
	if _, err := cmd.WriteTo(bw); err != nil {
		t.Error(err)
	}
	bw.Flush()

	t.Log(buf.String())

	pcmd := new(Request)
	_, err := pcmd.ReadFrom(bufio.NewReader(buf))
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 2, len(pcmd.data))
	assert.Equal(t, "hello", pcmd.data[0])
	assert.Equal(t, "redis", pcmd.data[1])
}
