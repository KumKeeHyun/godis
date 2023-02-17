package command

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type encoded struct {
	Cmd  string
	Data []byte
}

func Marshal(cmd WriteCommand) ([]byte, error) {
	d, err := cmd.Marshal()
	if err != nil {
		return nil, err
	}

	enc := encoded{
		Cmd:  cmd.Command(),
		Data: d,
	}
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(enc)
	return buf.Bytes(), err
}

func Unmarshal(b []byte) (WriteCommand, error) {
	var enc encoded
	err := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&enc)
	if err != nil {
		return nil, err
	}

	cmd := getWriteCommand(enc.Cmd)
	if cmd == nil {
		return nil, fmt.Errorf("command: failed to unmarshal cmd %s", enc.Cmd)
	}
	err = cmd.Unmarshal(enc.Data)
	return cmd, err
}

func getWriteCommand(cmd string) WriteCommand {
	switch cmd {
	case "set":
		return &Set{}
	default:
		return nil
	}
}
